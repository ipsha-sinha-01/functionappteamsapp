import os
import json
import uuid
import logging
from datetime import datetime, timedelta
from typing import Optional

import aiohttp
import azure.functions as func

from botbuilder.core import (
    BotFrameworkAdapter,
    BotFrameworkAdapterSettings,
    TurnContext,
    CardFactory,
    MessageFactory,
)
from botbuilder.core.teams import TeamsActivityHandler
from botbuilder.schema import (
    Activity,
    InvokeResponse,
    OAuthCard,
    CardAction,
)
from botframework.connector.token_api.models import TokenExchangeRequest

# ====================== Logging ======================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("teams_bot")

# ====================== Environment Variables ======================
BOT_APP_ID = os.getenv("BOT_APP_ID", "")
BOT_APP_PASSWORD = os.getenv("BOT_APP_PASSWORD", "")
BOT_APP_TENANT_ID = os.getenv("BOT_APP_TENANT_ID", "")

DATABRICKS_WORKSPACE_URL = os.getenv("DATABRICKS_WORKSPACE_URL", "").rstrip("/")
DATABRICKS_SERVING_ENDPOINT_NAME = os.getenv("DATABRICKS_SERVING_ENDPOINT_NAME", "")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "")  # optional PAT fallback

# OAuth connection name configured in Azure Bot -> OAuth Connection Settings
CONNECTION_NAME = os.getenv("CONNECTION_NAME", "")

CHANNEL_AUTH_TENANT = os.getenv("CHANNEL_AUTH_TENANT", BOT_APP_TENANT_ID)

# Databricks AAD scope for OBO
DATABRICKS_AAD_SCOPE = os.getenv(
    "DATABRICKS_AAD_SCOPE",
    "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default"
)

# If true, the bot will refuse to call Databricks without a user token.
# This is the recommended setting for your use case.
REQUIRE_USER_AUTH = os.getenv("REQUIRE_USER_AUTH", "true").lower() == "true"

# If REQUIRE_USER_AUTH=false and this is true, PAT can be used as fallback.
ALLOW_PAT_FALLBACK = os.getenv("ALLOW_PAT_FALLBACK", "false").lower() == "true"

INACTIVITY_THRESHOLD_MINUTES = int(os.getenv("INACTIVITY_THRESHOLD_MINUTES", "2"))
USER_TOKEN_CACHE_MINUTES = int(os.getenv("USER_TOKEN_CACHE_MINUTES", "50"))

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# ====================== Adapter Setup ======================
settings = BotFrameworkAdapterSettings(
    app_id=BOT_APP_ID,
    app_password=BOT_APP_PASSWORD,
    channel_auth_tenant=CHANNEL_AUTH_TENANT,
)
adapter = BotFrameworkAdapter(settings)

# ====================== In-memory Stores ======================
# NOTE: For production replace these with Redis / external store.
conversation_store = {}
feedback_store = {}
user_token_store = {}


# ====================== Logging Helpers ======================
def mask_value(value: Optional[str], visible_prefix: int = 6, visible_suffix: int = 4) -> str:
    if value is None:
        return "<None>"

    value = str(value)
    if value == "":
        return "<empty>"

    if len(value) <= visible_prefix + visible_suffix:
        return "*" * len(value)

    return f"{value[:visible_prefix]}...{value[-visible_suffix:]}"


def log_runtime_configuration():
    logger.info("Runtime configuration loaded")
    logger.info("BOT_APP_ID: %s", mask_value(BOT_APP_ID))
    logger.info("BOT_APP_PASSWORD configured: %s", bool(BOT_APP_PASSWORD))
    logger.info("BOT_APP_PASSWORD length: %s", len(BOT_APP_PASSWORD) if BOT_APP_PASSWORD else 0)
    logger.info("BOT_APP_TENANT_ID: %s", mask_value(BOT_APP_TENANT_ID))
    logger.info("CHANNEL_AUTH_TENANT: %s", mask_value(CHANNEL_AUTH_TENANT))
    logger.info("CONNECTION_NAME: %s", CONNECTION_NAME or "<empty>")
    logger.info("DATABRICKS_WORKSPACE_URL: %s", DATABRICKS_WORKSPACE_URL or "<empty>")
    logger.info(
        "DATABRICKS_SERVING_ENDPOINT_NAME: %s",
        DATABRICKS_SERVING_ENDPOINT_NAME or "<empty>",
    )
    logger.info("DATABRICKS_TOKEN configured: %s", bool(DATABRICKS_TOKEN))
    logger.info("DATABRICKS_TOKEN preview: %s", mask_value(DATABRICKS_TOKEN))
    logger.info("DATABRICKS_AAD_SCOPE: %s", DATABRICKS_AAD_SCOPE)
    logger.info("REQUIRE_USER_AUTH: %s", REQUIRE_USER_AUTH)
    logger.info("ALLOW_PAT_FALLBACK: %s", ALLOW_PAT_FALLBACK)
    logger.info("INACTIVITY_THRESHOLD_MINUTES: %s", INACTIVITY_THRESHOLD_MINUTES)
    logger.info("USER_TOKEN_CACHE_MINUTES: %s", USER_TOKEN_CACHE_MINUTES)


def safe_json_dumps(obj) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, default=str)
    except Exception as e:
        return f"<<json serialization failed: {str(e)}>>"


def serialize_activity_for_log(activity: Activity) -> dict:
    try:
        if hasattr(activity, "serialize"):
            return activity.serialize()
        return {"warning": "activity has no serialize()", "repr": str(activity)}
    except Exception as e:
        return {"error": f"activity serialize failed: {str(e)}", "repr": str(activity)}


def log_outgoing_activity(prefix: str, activity: Activity):
    try:
        serialized = serialize_activity_for_log(activity)
        logger.info("%s serialized activity: %s", prefix, safe_json_dumps(serialized))
    except Exception:
        logger.exception("Failed while logging outgoing activity")


# ====================== Auth Cache Helpers ======================
def build_auth_store_key(turn_context: TurnContext) -> str:
    tenant_id = ""
    try:
        tenant_id = (
            (turn_context.activity.channel_data or {})
            .get("tenant", {})
            .get("id", "")
        )
    except Exception:
        tenant_id = ""

    user_id = turn_context.activity.from_property.id or ""
    conversation_id = turn_context.activity.conversation.id or ""
    logger.info(
        "Building auth store key with tenant_id=%s user_id=%s conversation_id=%s",
        mask_value(tenant_id),
        mask_value(user_id),
        mask_value(conversation_id),
    )
    return f"{tenant_id}:{user_id}:{conversation_id}"


def cache_databricks_user_token(turn_context: TurnContext, databricks_token: str):
    key = build_auth_store_key(turn_context)
    user_token_store[key] = {
        "databricks_token": databricks_token,
        "expires_at": (datetime.utcnow() + timedelta(minutes=USER_TOKEN_CACHE_MINUTES)).isoformat(),
        "updated_at": datetime.utcnow().isoformat(),
    }
    logger.info(
        "Cached Databricks user token for key=%s token=%s expires_at=%s",
        mask_value(key),
        mask_value(databricks_token),
        user_token_store[key]["expires_at"],
    )


def get_cached_databricks_user_token(turn_context: TurnContext) -> Optional[str]:
    key = build_auth_store_key(turn_context)
    record = user_token_store.get(key)
    logger.info("Checking cached Databricks token for key: %s", mask_value(key))

    if not record:
        logger.info("No cached Databricks token found for key: %s", mask_value(key))
        return None

    try:
        expires_at = datetime.fromisoformat(record["expires_at"])
    except Exception:
        logger.warning("Invalid expires_at in token cache; dropping cached token")
        user_token_store.pop(key, None)
        return None

    if datetime.utcnow() >= expires_at:
        logger.info("Cached user token expired for key: %s", key)
        user_token_store.pop(key, None)
        return None

    logger.info(
        "Cached Databricks token is valid for key=%s until=%s token=%s",
        mask_value(key),
        record.get("expires_at"),
        mask_value(record.get("databricks_token")),
    )
    return record.get("databricks_token")


def clear_cached_databricks_user_token(turn_context: TurnContext):
    key = build_auth_store_key(turn_context)
    user_token_store.pop(key, None)
    logger.info("Cleared cached Databricks user token for key: %s", mask_value(key))


# ====================== SSO / OBO Token Helpers ======================
async def exchange_token_obo(sso_token: str, target_scope: str) -> Optional[str]:
    """
    Exchange the Teams/Bot Framework user token for a downstream token
    scoped to Databricks using Azure AD OBO flow.
    """
    try:
        tenant_id = BOT_APP_TENANT_ID
        client_id = BOT_APP_ID
        client_secret = BOT_APP_PASSWORD

        if not all([tenant_id, client_id, client_secret]):
            logger.error("Missing AAD credentials for OBO exchange")
            logger.info(
                "OBO config check tenant=%s client_id=%s client_secret_configured=%s",
                mask_value(tenant_id),
                mask_value(client_id),
                bool(client_secret),
            )
            return None

        url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        logger.info("OBO token exchange URL: %s", url)
        logger.info("OBO target_scope: %s", target_scope)
        logger.info("OBO incoming SSO token preview: %s", mask_value(sso_token))

        payload = {
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "client_id": client_id,
            "client_secret": client_secret,
            "assertion": sso_token,
            "scope": target_scope,
            "requested_token_use": "on_behalf_of",
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=payload) as resp:
                data = await resp.json()
                logger.info("OBO exchange HTTP status: %s", resp.status)

                if resp.status == 200:
                    token = data.get("access_token")
                    logger.info("OBO token exchange succeeded for scope: %s", target_scope)
                    logger.info("OBO token preview: %s", mask_value(token))
                    return token

                logger.error(
                    "OBO exchange failed (HTTP %s): %s",
                    resp.status,
                    safe_json_dumps(data),
                )
                return None

    except Exception:
        logger.exception("OBO token exchange threw an exception")
        return None


async def get_databricks_user_token_from_bf_token(bf_user_token: str) -> Optional[str]:
    return await exchange_token_obo(bf_user_token, DATABRICKS_AAD_SCOPE)


def extract_token_from_activity_value(activity: Activity) -> Optional[str]:
    try:
        value = activity.value or {}
        logger.info("Extracting token from activity.value: %s", safe_json_dumps(value))
        if isinstance(value, dict):
            if value.get("token"):
                logger.info("Token found directly in activity.value.token")
                return value.get("token")

            authentication = value.get("authentication") or {}
            if isinstance(authentication, dict) and authentication.get("token"):
                logger.info("Token found in activity.value.authentication.token")
                return authentication.get("token")

        logger.info("No token present in activity.value payload")
        return None
    except Exception:
        logger.exception("Failed to extract token from activity value")
        return None


async def send_sign_in_card(turn_context: TurnContext):
    """
    Sends an OAuth card so the user can sign in.
    Requires CONNECTION_NAME to be configured in Azure Bot.
    """
    if not CONNECTION_NAME:
        logger.warning("Cannot send OAuth sign-in card because CONNECTION_NAME is empty")
        await turn_context.send_activity(
            "User sign-in is required, but CONNECTION_NAME is not configured on the bot."
        )
        return

    sign_in_resource = None
    sign_in_link = None
    token_exchange_resource = None
    token_post_resource = None

    try:
        sign_in_resource = await adapter.get_sign_in_resource_from_user(
            turn_context,
            CONNECTION_NAME,
            turn_context.activity.from_property.id,
        )
        sign_in_link = getattr(sign_in_resource, "sign_in_link", None)
        token_exchange_resource = getattr(sign_in_resource, "token_exchange_resource", None)
        token_post_resource = getattr(sign_in_resource, "token_post_resource", None)
        logger.info(
            "Retrieved sign-in resource sign_in_link=%s token_exchange_resource=%s token_post_resource=%s",
            sign_in_link,
            safe_json_dumps(
                token_exchange_resource.serialize()
                if token_exchange_resource and hasattr(token_exchange_resource, "serialize")
                else token_exchange_resource
            ),
            safe_json_dumps(
                token_post_resource.serialize()
                if token_post_resource and hasattr(token_post_resource, "serialize")
                else token_post_resource
            ),
        )
    except Exception:
        logger.exception("Failed to retrieve sign-in resource from Bot Framework")

    oauth_card = OAuthCard(
        text="Please sign in to continue.",
        connection_name=CONNECTION_NAME,
        buttons=(
            [CardAction(type="signin", title="Sign in", value=sign_in_link)]
            if sign_in_link
            else None
        ),
        token_exchange_resource=token_exchange_resource,
        token_post_resource=token_post_resource,
    )
    logger.info("Sending OAuth card with connection name: %s", CONNECTION_NAME)
    oauth_attachment = CardFactory.oauth_card(oauth_card)
    logger.info(
        "OAuth card attachment payload: %s",
        safe_json_dumps(oauth_attachment.serialize() if hasattr(oauth_attachment, "serialize") else str(oauth_attachment)),
    )

    oauth_activity = MessageFactory.attachment(oauth_attachment)
    log_outgoing_activity("OAuth sign-in activity", oauth_activity)
    await turn_context.send_activity(oauth_activity)
    logger.info("OAuth sign-in card sent")


async def resolve_user_databricks_token(turn_context: TurnContext) -> Optional[str]:
    """
    Returns a Databricks user token if available.
    Order:
      1. token cache
      2. Bot Framework get_user_token using OAuth connection
      3. None
    """
    cached = get_cached_databricks_user_token(turn_context)
    if cached:
        logger.info("Using cached Databricks user token")
        return cached

    if not CONNECTION_NAME:
        logger.warning("CONNECTION_NAME not configured; cannot retrieve user token from BF")
        return None

    logger.info("Attempting Bot Framework get_user_token with connection: %s", CONNECTION_NAME)
    try:
        token_response = await adapter.get_user_token(turn_context, CONNECTION_NAME, None)
    except Exception:
        logger.exception("adapter.get_user_token failed")
        return None

    logger.info(
        "adapter.get_user_token returned object=%s has_token=%s",
        type(token_response).__name__ if token_response else None,
        bool(token_response and getattr(token_response, "token", None)),
    )

    if not token_response or not getattr(token_response, "token", None):
        logger.info("No Bot Framework user token available yet")
        return None

    logger.info("Bot Framework user token retrieved successfully")
    logger.info("Bot Framework user token preview: %s", mask_value(token_response.token))

    databricks_token = await get_databricks_user_token_from_bf_token(token_response.token)
    if not databricks_token:
        logger.warning("Got user token from Bot Framework but OBO to Databricks failed")
        return None

    cache_databricks_user_token(turn_context, databricks_token)
    return databricks_token


# ====================== Adaptive Cards ======================
def build_model_dropdown_card(selected_value: str = "fast"):
    return {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.5",
        "body": [
            {
                "type": "TextBlock",
                "text": "Hello, please select a model for your query.",
                "weight": "Bolder",
                "size": "Medium",
                "wrap": True,
            },
            {
                "type": "Input.ChoiceSet",
                "id": "model",
                "style": "compact",
                "value": selected_value,
                "choices": [
                    {"title": "Fast", "value": "fast"},
                    {"title": "Thinking", "value": "thinking"},
                    {"title": "Pro", "value": "pro"},
                    {"title": "Ultra", "value": "ultra"},
                ],
            },
        ],
        "actions": [
            {
                "type": "Action.Submit",
                "title": "Apply",
                "data": {"action": "setModel"},
            }
        ],
    }


def build_model_task_module_card():
    return {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.5",
        "body": [
            {
                "type": "TextBlock",
                "text": "Ask with Model",
                "weight": "Bolder",
                "size": "Large",
            },
            {
                "type": "Input.Text",
                "id": "userMessage",
                "placeholder": "Type your question or message here...",
                "isMultiline": True,
                "height": "stretch",
            },
            {
                "type": "Input.ChoiceSet",
                "id": "model",
                "label": "Select Model",
                "style": "compact",
                "value": "fast",
                "choices": [
                    {"title": "Fast", "value": "fast"},
                    {"title": "Thinking", "value": "thinking"},
                    {"title": "Pro", "value": "pro"},
                    {"title": "Ultra", "value": "ultra"},
                ],
            },
        ],
        "actions": [
            {
                "type": "Action.Submit",
                "title": "Send",
                "data": {"action": "sendWithModel"},
            }
        ],
    }


def build_feedback_card(feedback_id: str):
    return {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.5",
        "body": [],
        "actions": [
            {
                "type": "Action.Submit",
                "title": "👍 Helpful",
                "data": {
                    "action": "feedback_up",
                    "feedback_id": feedback_id,
                },
            },
            {
                "type": "Action.Submit",
                "title": "👎 Not Helpful",
                "data": {
                    "action": "open_disagree_dialog",
                    "feedback_id": feedback_id,
                    "msteams": {"type": "task/fetch"},
                },
            },
        ],
    }


def build_disagree_dialog_card(feedback_id: str):
    return {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.5",
        "body": [
            {
                "type": "TextBlock",
                "text": "Help us improve",
                "weight": "Bolder",
                "size": "Medium",
                "wrap": True,
            },
            {
                "type": "TextBlock",
                "text": "What was the issue?",
                "wrap": True,
                "spacing": "Medium",
            },
            {
                "type": "Input.ChoiceSet",
                "id": "reason",
                "style": "expanded",
                "isRequired": True,
                "errorMessage": "Please select one reason.",
                "choices": [
                    {"title": "Incorrect", "value": "incorrect"},
                    {"title": "Incomplete", "value": "incomplete"},
                    {"title": "Not relevant", "value": "not_relevant"},
                    {"title": "Too generic", "value": "too_generic"},
                    {"title": "Other", "value": "other"},
                ],
            },
            {
                "type": "Input.Text",
                "id": "comment",
                "label": "Additional comments (optional)",
                "placeholder": "Tell us what could be improved...",
                "isMultiline": True,
            },
        ],
        "actions": [
            {
                "type": "Action.Submit",
                "title": "Submit",
                "data": {
                    "action": "submit_disagree_feedback",
                    "feedback_id": feedback_id,
                },
            }
        ],
    }


# ====================== AI Message Builder ======================
def build_ai_enhanced_message(text: str) -> Activity:
    return Activity(
        type="message",
        text=text,
        text_format="markdown",
        channel_data={
            "entities": [
                {
                    "type": "https://schema.org/Message",
                    "@type": "Message",
                    "@context": "https://schema.org",
                    "additionalType": ["AIGeneratedContent"],
                }
            ]
        },
    )


# ====================== Task Module Helpers ======================
def wrap_adaptive_card_for_task_module(card_json: dict) -> dict:
    return {
        "contentType": "application/vnd.microsoft.card.adaptive",
        "content": card_json,
    }


def build_task_module_continue_response(
    card_json: dict,
    title: str,
    height: int,
    width: int,
) -> InvokeResponse:
    return InvokeResponse(
        status=200,
        body={
            "task": {
                "type": "continue",
                "value": {
                    "title": title,
                    "height": height,
                    "width": width,
                    "card": wrap_adaptive_card_for_task_module(card_json),
                },
            }
        },
    )


# ====================== Databricks Helpers ======================
def build_databricks_url() -> str:
    if not DATABRICKS_WORKSPACE_URL:
        raise ValueError("DATABRICKS_WORKSPACE_URL is not configured")
    if not DATABRICKS_SERVING_ENDPOINT_NAME:
        raise ValueError("DATABRICKS_SERVING_ENDPOINT_NAME is not configured")
    return (
        f"{DATABRICKS_WORKSPACE_URL}/serving-endpoints/"
        f"{DATABRICKS_SERVING_ENDPOINT_NAME}/invocations"
    )


async def call_databricks_stream_collect(
    user_text: str,
    conversation_id: str,
    user_id: str,
    model: str,
    token: Optional[str] = None,
) -> str:
    try:
        url = build_databricks_url()

        auth_token = token
        token_type = "user-OBO"

        if not auth_token and ALLOW_PAT_FALLBACK and DATABRICKS_TOKEN:
            auth_token = DATABRICKS_TOKEN
            token_type = "PAT"

        if not auth_token:
            return "❌ No Databricks token available."

        payload = {
            "input": [{"role": "user", "content": user_text}],
            "custom_inputs": {
                "session_id": conversation_id,
                "user_id": user_id,
                "model": model,
            },
            "stream": True,
        }

        headers = {
            "Authorization": f"Bearer {auth_token}",
            "Content-Type": "application/json",
        }

        logger.info("Calling Databricks (token type: %s)", token_type)
        logger.info("Databricks auth token preview: %s", mask_value(auth_token))
        logger.info("Databricks request conversation_id: %s", mask_value(conversation_id))
        logger.info("Databricks request user_id: %s", mask_value(user_id))
        logger.info("Databricks request model: %s", model)
        logger.info("Payload: %s", safe_json_dumps(payload))

        accumulated = ""
        timeout = aiohttp.ClientTimeout(total=180)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, headers=headers, json=payload) as resp:
                logger.info("Databricks status: %s", resp.status)

                if resp.status == 401 and token_type == "user-OBO":
                    logger.warning("Databricks rejected cached user token with 401")
                    return "❌ Databricks rejected the user token."

                if resp.status != 200:
                    error_text = await resp.text()
                    logger.error("Databricks error: %s", error_text)
                    return f"❌ Databricks error ({resp.status}): {error_text[:500]}"

                async for raw_line in resp.content:
                    line = raw_line.decode("utf-8", errors="ignore").strip()
                    if not line:
                        continue

                    if line.startswith("data: "):
                        line = line[6:].strip()

                    if line in ("[DONE]", "data: [DONE]"):
                        break

                    if not line:
                        continue

                    try:
                        data = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    event_type = data.get("type", "")
                    if event_type == "response.output_text.delta":
                        delta = data.get("delta", "")
                        if delta:
                            accumulated += delta
                    elif event_type == "response.output_item.done":
                        logger.info("Databricks stream completed (output_item.done)")

        final_text = accumulated.strip()
        return final_text or "No response received from Databricks."

    except Exception as e:
        logger.exception("Error calling Databricks streaming")
        return f"❌ Error calling Databricks: {str(e)}"


# ====================== Feedback Helper ======================
async def send_feedback_to_databricks(feedback_payload: dict):
    try:
        logger.info("Feedback payload: %s", safe_json_dumps(feedback_payload))
    except Exception:
        logger.exception("Error while logging feedback payload")


# ====================== Shared Response Flow ======================
async def respond_with_databricks(
    turn_context: TurnContext,
    user_text: str,
    model: str,
    user_token: Optional[str],
) -> None:
    conversation_id = turn_context.activity.conversation.id
    user_id = turn_context.activity.from_property.id

    await turn_context.send_activity("Received your message. Calling Databricks now...")

    final_text = await call_databricks_stream_collect(
        user_text=user_text,
        conversation_id=conversation_id,
        user_id=user_id,
        model=model,
        token=user_token,
    )

    if final_text == "❌ Databricks rejected the user token.":
        clear_cached_databricks_user_token(turn_context)
        await turn_context.send_activity(
            "Your session expired. Please sign in again."
        )
        await send_sign_in_card(turn_context)
        return

    ai_message = build_ai_enhanced_message(final_text)
    log_outgoing_activity("AI message", ai_message)
    sent_response = await turn_context.send_activity(ai_message)
    bot_activity_id = sent_response.id if sent_response else None

    feedback_id = str(uuid.uuid4())
    feedback_store[feedback_id] = {
        "feedback_id": feedback_id,
        "conversation_id": conversation_id,
        "user_id": user_id,
        "question": user_text,
        "answer": final_text,
        "model": model,
        "bot_activity_id": bot_activity_id,
        "auth_method": "obo" if user_token else ("pat" if ALLOW_PAT_FALLBACK else "none"),
        "created_at": datetime.utcnow().isoformat(),
    }

    feedback_card = build_feedback_card(feedback_id)
    await turn_context.send_activity(
        MessageFactory.attachment(CardFactory.adaptive_card(feedback_card))
    )


# ====================== Main Bot Class ======================
class MyTeamsBot(TeamsActivityHandler):
    async def on_turn(self, turn_context: TurnContext):
        activity = turn_context.activity
        logger.info(
            "on_turn activity type=%s name=%s channel=%s conversation_id=%s from_id=%s service_url=%s",
            activity.type,
            activity.name,
            activity.channel_id,
            mask_value(activity.conversation.id if activity.conversation else ""),
            mask_value(activity.from_property.id if activity.from_property else ""),
            activity.service_url,
        )

        # Teams SSO token exchange invoke
        if activity.type == "invoke" and activity.name == "signin/tokenExchange":
            await self._handle_signin_token_exchange(turn_context)
            return

        await super().on_turn(turn_context)

    async def _handle_signin_token_exchange(self, turn_context: TurnContext):
        """
        Handle the signin/tokenExchange invoke from Teams.
        We do OBO immediately and cache the Databricks user token.
        """
        try:
            logger.info("signin/tokenExchange received")
            logger.info(
                "signin/tokenExchange activity.value: %s",
                safe_json_dumps(turn_context.activity.value or {}),
            )
            invoke_value = turn_context.activity.value or {}
            exchange_id = invoke_value.get("id") if isinstance(invoke_value, dict) else None
            connection_name = (
                invoke_value.get("connectionName")
                if isinstance(invoke_value, dict) and invoke_value.get("connectionName")
                else CONNECTION_NAME
            )
            sso_token = extract_token_from_activity_value(turn_context.activity)

            if not sso_token:
                logger.warning("No token found in signin/tokenExchange payload")
                await turn_context.send_activity(
                    Activity(
                        type="invokeResponse",
                        value=InvokeResponse(status=400, body={"error": "no token"}),
                    )
                )
                return

            logger.info(
                "Attempting adapter.exchange_token for connection=%s exchange_id=%s",
                connection_name,
                exchange_id,
            )
            exchanged = await adapter.exchange_token(
                turn_context,
                connection_name,
                turn_context.activity.from_property.id,
                TokenExchangeRequest(token=sso_token),
            )
            logger.info(
                "adapter.exchange_token returned has_token=%s channel_id=%s",
                bool(exchanged and getattr(exchanged, "token", None)),
                getattr(exchanged, "channel_id", None) if exchanged else None,
            )

            bf_user_token = exchanged.token if exchanged and getattr(exchanged, "token", None) else sso_token
            databricks_token = await get_databricks_user_token_from_bf_token(bf_user_token)

            if databricks_token:
                cache_databricks_user_token(turn_context, databricks_token)
                logger.info("Databricks user token cached from signin/tokenExchange")
                await turn_context.send_activity(
                    Activity(
                        type="invokeResponse",
                        value=InvokeResponse(status=200, body={}),
                    )
                )
                return

            logger.warning("OBO failed during signin/tokenExchange")
            await turn_context.send_activity(
                Activity(
                    type="invokeResponse",
                    value=InvokeResponse(status=412, body={"error": "obo_failed"}),
                )
            )

        except Exception:
            logger.exception("Error in _handle_signin_token_exchange")
            await turn_context.send_activity(
                Activity(
                    type="invokeResponse",
                    value=InvokeResponse(status=500, body={"error": "token exchange failed"}),
                )
            )

    async def on_token_response_event(self, turn_context: TurnContext):
        """
        Handles OAuth token response event after user signs in via OAuth card.
        """
        try:
            logger.info("tokens/response event received")
            logger.info(
                "tokens/response activity.value: %s",
                safe_json_dumps(turn_context.activity.value or {}),
            )

            sso_token = extract_token_from_activity_value(turn_context.activity)
            if not sso_token:
                logger.warning("No token found in tokens/response payload")
                await turn_context.send_activity(
                    "Sign-in completed, but no token was received by the bot."
                )
                return

            databricks_token = await get_databricks_user_token_from_bf_token(sso_token)
            if not databricks_token:
                await turn_context.send_activity(
                    "Sign-in completed, but token exchange to Databricks failed."
                )
                return

            cache_databricks_user_token(turn_context, databricks_token)
            await turn_context.send_activity(
                "You are signed in now. Please send your question again."
            )

        except Exception:
            logger.exception("Error in on_token_response_event")
            await turn_context.send_activity("❌ Error while completing sign-in.")

    async def on_message_activity(self, turn_context: TurnContext):
        try:
            if turn_context.activity.value:
                await self.handle_card_submit(turn_context)
                return

            user_text = (turn_context.activity.text or "").strip()
            conversation_id = turn_context.activity.conversation.id
            user_id = turn_context.activity.from_property.id
            now = datetime.utcnow()

            logger.info("Incoming text: %s", user_text)
            logger.info("Conversation ID: %s", conversation_id)
            logger.info("User ID: %s", user_id)
            logger.info("AadObjectId: %s", getattr(turn_context.activity.from_property, "aad_object_id", None))
            logger.info("Activity value present on message: %s", bool(turn_context.activity.value))
            logger.info("Channel data: %s", safe_json_dumps(turn_context.activity.channel_data or {}))
            logger.info("Entities: %s", safe_json_dumps(turn_context.activity.entities or []))

            if conversation_id not in conversation_store:
                conversation_store[conversation_id] = {
                    "model": "fast",
                    "last_activity": now,
                }

            store = conversation_store[conversation_id]
            time_since_last = (now - store["last_activity"]).total_seconds() / 60
            is_new_session = time_since_last > INACTIVITY_THRESHOLD_MINUTES
            store["last_activity"] = now

            # Show model chooser first for new/empty/model-switch flows
            if (
                is_new_session
                or not user_text
                or user_text.lower() in ["change model", "select model", "choose model", "model"]
            ):
                card = build_model_dropdown_card(store["model"])
                await turn_context.send_activity(
                    MessageFactory.attachment(CardFactory.adaptive_card(card))
                )
                return

            user_token = await resolve_user_databricks_token(turn_context)
            logger.info("on_message_activity — user token available: %s", bool(user_token))

            if REQUIRE_USER_AUTH and not user_token:
                await send_sign_in_card(turn_context)
                return

            await respond_with_databricks(
                turn_context=turn_context,
                user_text=user_text,
                model=store["model"],
                user_token=user_token,
            )

        except Exception as e:
            logger.exception("Error in on_message_activity")
            await turn_context.send_activity(f"❌ Bot error: {str(e)}")

    async def handle_card_submit(self, turn_context: TurnContext):
        try:
            data = turn_context.activity.value or {}
            action = data.get("action")

            logger.info("Card submit payload: %s", safe_json_dumps(data))
            logger.info("Card submit action: %s", action)

            if action == "setModel":
                conversation_id = turn_context.activity.conversation.id
                selected_model = data.get("model", "fast")

                if conversation_id not in conversation_store:
                    conversation_store[conversation_id] = {
                        "model": selected_model,
                        "last_activity": datetime.utcnow(),
                    }
                else:
                    conversation_store[conversation_id]["model"] = selected_model
                    conversation_store[conversation_id]["last_activity"] = datetime.utcnow()

                await turn_context.send_activity(f"Model updated to: {selected_model}")
                return

            if action == "sendWithModel":
                user_message = (data.get("userMessage") or "").strip()
                selected_model = data.get("model", "fast")

                if not user_message:
                    await turn_context.send_activity("Please enter a message.")
                    return

                conversation_id = turn_context.activity.conversation.id
                conversation_store[conversation_id] = {
                    "model": selected_model,
                    "last_activity": datetime.utcnow(),
                }

                user_token = await resolve_user_databricks_token(turn_context)
                logger.info("handle_card_submit(sendWithModel) — user token available: %s", bool(user_token))

                if REQUIRE_USER_AUTH and not user_token:
                    await send_sign_in_card(turn_context)
                    return

                await respond_with_databricks(
                    turn_context=turn_context,
                    user_text=user_message,
                    model=selected_model,
                    user_token=user_token,
                )
                return

            if action == "feedback_up":
                feedback_id = data.get("feedback_id")
                logger.info("POSITIVE FEEDBACK — ID: %s", feedback_id)

                record = feedback_store.get(feedback_id, {})
                record["feedback_type"] = "up"
                record["submitted_at"] = datetime.utcnow().isoformat()
                feedback_store[feedback_id] = record

                await send_feedback_to_databricks(record)
                await turn_context.send_activity("Thanks for your feedback.")
                return

            await turn_context.send_activity("Action received.")

        except Exception as e:
            logger.exception("Error in handle_card_submit")
            await turn_context.send_activity(f"❌ Error handling card: {str(e)}")

    async def on_teams_messaging_extension_fetch_task(self, turn_context: TurnContext, action):
        try:
            card = build_model_task_module_card()
            return {
                "task": {
                    "type": "continue",
                    "value": {
                        "title": "Ask with Model",
                        "height": 380,
                        "width": 500,
                        "card": {
                            "contentType": "application/vnd.microsoft.card.adaptive",
                            "content": card,
                        },
                    },
                }
            }
        except Exception as e:
            logger.exception("Error in on_teams_messaging_extension_fetch_task")
            error_card = {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.5",
                "body": [
                    {
                        "type": "TextBlock",
                        "text": f"Unable to open task module: {str(e)}",
                        "wrap": True,
                        "color": "Attention",
                    }
                ],
            }
            return {
                "task": {
                    "type": "continue",
                    "value": {
                        "title": "Error",
                        "height": 200,
                        "width": 400,
                        "card": {
                            "contentType": "application/vnd.microsoft.card.adaptive",
                            "content": error_card,
                        },
                    },
                }
            }

    async def on_teams_messaging_extension_submit_action(self, turn_context: TurnContext, action):
        try:
            data = action.data or {}
            user_message = (data.get("userMessage") or "").strip()
            selected_model = data.get("model", "fast")

            if not user_message:
                await turn_context.send_activity("Please enter a message.")
                return

            conversation_id = turn_context.activity.conversation.id
            conversation_store[conversation_id] = {
                "model": selected_model,
                "last_activity": datetime.utcnow(),
            }

            user_token = await resolve_user_databricks_token(turn_context)
            logger.info(
                "on_teams_messaging_extension_submit_action — user token available: %s",
                bool(user_token),
            )

            if REQUIRE_USER_AUTH and not user_token:
                await send_sign_in_card(turn_context)
                return

            await respond_with_databricks(
                turn_context=turn_context,
                user_text=user_message,
                model=selected_model,
                user_token=user_token,
            )

        except Exception as e:
            logger.exception("Error in messaging extension submit")
            await turn_context.send_activity(f"Error: {str(e)}")

    async def on_invoke_activity(self, turn_context: TurnContext):
        try:
            invoke_name = turn_context.activity.name
            invoke_value = turn_context.activity.value or {}

            logger.info("Invoke name: %s", invoke_name)
            logger.info("Invoke value: %s", safe_json_dumps(invoke_value))
            logger.info("Invoke channel data: %s", safe_json_dumps(turn_context.activity.channel_data or {}))

            if invoke_name == "task/fetch":
                data = invoke_value.get("data", invoke_value) if isinstance(invoke_value, dict) else {}
                action = data.get("action")
                if action == "open_disagree_dialog":
                    feedback_id = data.get("feedback_id")
                    dialog_card = build_disagree_dialog_card(feedback_id)
                    return build_task_module_continue_response(
                        card_json=dialog_card,
                        title="Help us improve",
                        height=420,
                        width=500,
                    )

            if invoke_name == "task/submit":
                data = invoke_value.get("data", invoke_value) if isinstance(invoke_value, dict) else {}
                action = data.get("action")
                if action == "submit_disagree_feedback":
                    feedback_id = data.get("feedback_id")
                    reason = invoke_value.get("reason")
                    comment = invoke_value.get("comment")

                    logger.info("NEGATIVE FEEDBACK — ID: %s reason: %s", feedback_id, reason)

                    record = feedback_store.get(feedback_id, {})
                    record["feedback_type"] = "down"
                    record["reason"] = reason
                    record["comment"] = comment
                    record["submitted_at"] = datetime.utcnow().isoformat()
                    feedback_store[feedback_id] = record

                    await send_feedback_to_databricks(record)

                    return InvokeResponse(
                        status=200,
                        body={"task": {"type": "message", "value": "Thanks for your feedback."}},
                    )

            if invoke_name == "adaptiveCard/action":
                action_data = invoke_value.get("action", {}) if isinstance(invoke_value, dict) else {}
                inner_data = action_data.get("data", {}) if isinstance(action_data, dict) else {}

                if inner_data:
                    payload = dict(inner_data)
                    body = action_data.get("body", {})
                    if isinstance(body, dict):
                        payload.update(body)
                    await self.handle_card_submit_with_payload(turn_context, payload)

                return InvokeResponse(status=200, body={"status": "OK"})

            return await super().on_invoke_activity(turn_context)

        except Exception as e:
            logger.exception("Error in on_invoke_activity")
            return InvokeResponse(status=500, body={"error": str(e)})

    async def handle_card_submit_with_payload(self, turn_context: TurnContext, payload: dict):
        try:
            action = payload.get("action")

            if action == "setModel":
                conversation_id = turn_context.activity.conversation.id
                selected_model = payload.get("model", "fast")

                if conversation_id not in conversation_store:
                    conversation_store[conversation_id] = {
                        "model": selected_model,
                        "last_activity": datetime.utcnow(),
                    }
                else:
                    conversation_store[conversation_id]["model"] = selected_model
                    conversation_store[conversation_id]["last_activity"] = datetime.utcnow()

                await turn_context.send_activity(f"Model updated to: {selected_model}")
                return

            if action == "sendWithModel":
                user_message = (payload.get("userMessage") or "").strip()
                selected_model = payload.get("model", "fast")

                if not user_message:
                    await turn_context.send_activity("Please enter a message.")
                    return

                conversation_id = turn_context.activity.conversation.id
                conversation_store[conversation_id] = {
                    "model": selected_model,
                    "last_activity": datetime.utcnow(),
                }

                user_token = await resolve_user_databricks_token(turn_context)
                logger.info(
                    "handle_card_submit_with_payload(sendWithModel) — user token available: %s",
                    bool(user_token),
                )

                if REQUIRE_USER_AUTH and not user_token:
                    await send_sign_in_card(turn_context)
                    return

                await respond_with_databricks(
                    turn_context=turn_context,
                    user_text=user_message,
                    model=selected_model,
                    user_token=user_token,
                )
                return

            if action == "feedback_up":
                feedback_id = payload.get("feedback_id")

                record = feedback_store.get(feedback_id, {})
                record["feedback_type"] = "up"
                record["submitted_at"] = datetime.utcnow().isoformat()
                feedback_store[feedback_id] = record

                await send_feedback_to_databricks(record)
                await turn_context.send_activity("Thanks for your feedback.")
                return

            await turn_context.send_activity("Action received.")

        except Exception as e:
            logger.exception("Error in handle_card_submit_with_payload")
            await turn_context.send_activity(f"❌ Error: {str(e)}")


# Create bot instance
bot = MyTeamsBot()
log_runtime_configuration()


# ====================== Azure Function Entry Point ======================
@app.route(route="messages", methods=["POST"])
async def messages(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logger.info("messages endpoint called")

        body_bytes = req.get_body()
        if not body_bytes:
            return func.HttpResponse("Empty request body", status_code=400)

        body_str = body_bytes.decode("utf-8")
        activity_json = json.loads(body_str)
        logger.info("Incoming activity JSON: %s", safe_json_dumps(activity_json))

        activity = Activity().deserialize(activity_json)
        auth_header = req.headers.get("Authorization", "")
        logger.info("Authorization header present: %s", bool(auth_header))
        logger.info("Authorization header preview: %s", mask_value(auth_header))
        invoke_response = await adapter.process_activity(
            activity,
            auth_header,
            bot.on_turn,
        )

        if invoke_response:
            return func.HttpResponse(
                body=json.dumps(invoke_response.body),
                status_code=invoke_response.status,
                mimetype="application/json",
            )

        return func.HttpResponse(status_code=200)

    except Exception as e:
        logger.exception("Error in messages endpoint")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)
