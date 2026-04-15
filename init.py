import os
import json
import uuid
import logging
from datetime import datetime, timezone
from typing import Dict, Any

import aiohttp
import jwt
import azure.functions as func

from botbuilder.core import (
    BotFrameworkAdapter,
    BotFrameworkAdapterSettings,
    TurnContext,
    CardFactory,
    MessageFactory,
    MemoryStorage,
    ConversationState,
)
from botbuilder.core.teams import TeamsActivityHandler
from botbuilder.schema import (
    Activity,
    Attachment,
    OAuthCard,
    CardAction,
    ActionTypes,
)
from botframework.connector.token_api.models import TokenExchangeResource


# ====================== Logging ======================
logger = logging.getLogger("teams_bot")
logger.setLevel(logging.INFO)

if not logger.handlers:
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)


# ====================== Environment Variables ======================
BOT_APP_ID = os.getenv("BOT_APP_ID", "")
BOT_APP_PASSWORD = os.getenv("BOT_APP_PASSWORD", "")

DATABRICKS_WORKSPACE_URL = os.getenv(
    "DATABRICKS_WORKSPACE_URL",
    "https://",
).rstrip("/")
DATABRICKS_SERVING_ENDPOINT_NAME = os.getenv(
    "DATABRICKS_SERVING_ENDPOINT_NAME",
    "aria_model",
)
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "")

CHANNEL_AUTH_TENANT = os.getenv(
    "CHANNEL_AUTH_TENANT",
    "",
)
FUNCTION_BASE_URL = os.getenv(
    "FUNCTION_BASE_URL",
    "",
).rstrip("/")

CONNECTION_NAME = os.getenv("CONNECTION_NAME", "BotAADConnection")

# Databricks resource ID – same for AWS-hosted workspaces
DATABRICKS_RESOURCE_ID = ""


# ====================== Azure Function App ======================
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


# ====================== Adapter ======================
adapter_settings = BotFrameworkAdapterSettings(
    app_id=BOT_APP_ID,
    app_password=BOT_APP_PASSWORD,
    channel_auth_tenant=CHANNEL_AUTH_TENANT or None,
)
adapter = BotFrameworkAdapter(adapter_settings)


# ====================== State ======================
memory_storage = MemoryStorage()
conversation_state = ConversationState(memory_storage)
BOT_DATA = "BotData"
bot_data_accessor = conversation_state.create_property(BOT_DATA)

feedback_store: Dict[str, Dict[str, Any]] = {}

# In-memory token cache (replace with Redis / Table Storage for production)
USER_TOKEN_STORE: Dict[str, str] = {}  # teams_user_id -> access_token


# ====================== Helpers ======================
def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def safe_json_dumps(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, default=str)
    except Exception as exc:
        return f"<<json serialization failed: {str(exc)}>>"


async def get_bot_data(turn_context: TurnContext) -> Dict[str, Any]:
    try:
        data = await bot_data_accessor.get(turn_context)
        if not data or not isinstance(data, dict):
            data = {}
        return data
    except Exception:
        logger.exception("Failed to read bot data. Using empty dict.")
        return {}


def build_databricks_url() -> str:
    return (
        f"{DATABRICKS_WORKSPACE_URL}/serving-endpoints/"
        f"{DATABRICKS_SERVING_ENDPOINT_NAME}/invocations"
    )


def build_databricks_payload(
    user_text: str,
    conversation_id: str,
    user_id: str,
    model: str,
) -> dict:
    return {
        "input": [{"role": "user", "content": user_text}],
        "custom_inputs": {
            "session_id": conversation_id,
            "user_id": user_id,
            "model": model,
        },
        "stream": False,
    }


def extract_text_from_databricks_response(data: dict) -> str:
    try:
        if not isinstance(data, dict):
            return str(data)
        return (
            data.get("output_text")
            or data.get("text")
            or data.get("content")
            or data.get("answer")
            or safe_json_dumps(data)
        )
    except Exception:
        return str(data)


def build_model_dropdown_card(selected_value: str = "fast") -> dict:
    return {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.5",
        "body": [
            {
                "type": "TextBlock",
                "text": "Please select a model.",
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
            {"type": "Action.Submit", "title": "Apply", "data": {"action": "setModel"}}
        ],
    }


def build_feedback_card(feedback_id: str) -> dict:
    return {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.5",
        "body": [],
        "actions": [
            {
                "type": "Action.Submit",
                "title": "👍 Helpful",
                "data": {"action": "feedback_up", "feedback_id": feedback_id},
            },
            {
                "type": "Action.Submit",
                "title": "👎 Not Helpful",
                "data": {"action": "feedback_down", "feedback_id": feedback_id},
            },
        ],
    }


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
                    "additionalType": ["AIGeneratedContent"],
                }
            ]
        },
    )


def decode_token_claims(token: str) -> Dict[str, Any]:
    claims = jwt.decode(token, options={"verify_signature": False})
    return {
        "iss": claims.get("iss"),
        "aud": claims.get("aud"),
        "tid": claims.get("tid"),
        "oid": claims.get("oid"),
        "scp": claims.get("scp"),
        "upn": claims.get("upn"),
        "preferred_username": claims.get("preferred_username"),
        "name": claims.get("name"),
        "exp": claims.get("exp"),
    }


# ====================== OBO Token Exchange ======================
async def get_obo_databricks_token(user_assertion: str) -> str:
    """
    Exchange the user's AAD token (scoped to this bot app) for a
    Databricks-scoped token using the OAuth 2.0 On-Behalf-Of flow.
    """
    token_url = (
        f"https://login.microsoftonline.com/{CHANNEL_AUTH_TENANT}/oauth2/v2.0/token"
    )
    form = {
        "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
        "client_id": BOT_APP_ID,
        "client_secret": BOT_APP_PASSWORD,
        "assertion": user_assertion,
        "requested_token_use": "on_behalf_of",
        "scope": f"{DATABRICKS_RESOURCE_ID}/.default",
    }

    logger.info("Starting OBO exchange against Entra")

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        async with session.post(
            token_url,
            data=form,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        ) as resp:
            body = await resp.text()

            if resp.status != 200:
                logger.error(
                    "OBO exchange failed. status=%s body=%s",
                    resp.status,
                    body[:1500],
                )
                raise RuntimeError(f"OBO failed ({resp.status}): {body[:500]}")

            data = json.loads(body)
            logger.info("OBO exchange succeeded")
            return data["access_token"]


# ====================== Sign-In Card ======================
def build_signin_card() -> Attachment:
    """
    Sends an OAuthCard that allows Teams silent SSO via tokenExchangeResource.
    Keep this clean. Do not manually build a custom sign-in URL here.
    """
    oauth_card = OAuthCard(
        text="Please sign in to continue.",
        connection_name=CONNECTION_NAME,
        token_exchange_resource=TokenExchangeResource(
            id=str(uuid.uuid4()),
            uri=f"api://{BOT_APP_ID}",
        ),
    )
    return CardFactory.oauth_card(oauth_card)


# ====================== Databricks Call ======================
async def call_databricks_with_token(
    bearer_token: str,
    user_text: str,
    conversation_id: str,
    user_id: str,
    model: str,
) -> str:
    try:
        headers = {
            "Authorization": f"Bearer {bearer_token}",
            "Content-Type": "application/json",
        }
        url = build_databricks_url()
        payload = build_databricks_payload(user_text, conversation_id, user_id, model)

        logger.info(
            "Calling Databricks. conversation_id=%s user_id=%s model=%s text_len=%s url=%s",
            conversation_id,
            user_id,
            model,
            len(user_text or ""),
            url,
        )
        logger.info("Databricks payload=%s", safe_json_dumps(payload)[:1000])

        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=180)) as session:
            async with session.post(url, headers=headers, json=payload) as resp:
                response_text = await resp.text()
                logger.info("Databricks response status=%s", resp.status)

                if resp.status != 200:
                    logger.error("Databricks error body=%s", response_text[:1500])
                    return f"❌ Databricks error ({resp.status})\n{response_text[:1000]}"

                try:
                    data = json.loads(response_text)
                except json.JSONDecodeError:
                    logger.info("Databricks returned non-JSON body")
                    return response_text

                return extract_text_from_databricks_response(data)

    except Exception as exc:
        logger.exception("Databricks call failed")
        return f"❌ Error calling Databricks: {str(exc)}"


async def send_databricks_response(
    turn_context: TurnContext,
    token: str,
    user_text: str,
) -> None:
    data = await get_bot_data(turn_context)
    model = data.get("model", "fast")
    conversation_id = turn_context.activity.conversation.id
    user_id = turn_context.activity.from_property.id

    # For POC, using token directly
    databricks_token = token

    final_text = await call_databricks_with_token(
        bearer_token=databricks_token,
        user_text=user_text,
        conversation_id=conversation_id,
        user_id=user_id,
        model=model,
    )

    ai_message = build_ai_enhanced_message(final_text)
    sent_response = await turn_context.send_activity(ai_message)

    feedback_id = str(uuid.uuid4())
    feedback_store[feedback_id] = {
        "feedback_id": feedback_id,
        "conversation_id": conversation_id,
        "user_id": user_id,
        "question": user_text,
        "answer": final_text,
        "model": model,
        "bot_activity_id": sent_response.id if sent_response else None,
        "created_at": utcnow().isoformat(),
    }

    logger.info(
        "Response sent. feedback_id=%s bot_activity_id=%s",
        feedback_id,
        sent_response.id if sent_response else None,
    )

    await turn_context.send_activity(
        MessageFactory.attachment(
            CardFactory.adaptive_card(build_feedback_card(feedback_id))
        )
    )


async def log_outgoing_activities(turn_context: TurnContext, activities, next_send):
    try:
        for i, activity in enumerate(activities):
            logger.info(
                "OUTGOING activity[%s] type=%s name=%s text=%s attachment_count=%s",
                i,
                getattr(activity, "type", None),
                getattr(activity, "name", None),
                getattr(activity, "text", None),
                len(getattr(activity, "attachments", []) or []),
            )
    except Exception:
        logger.exception("Failed while logging outgoing activities")

    return await next_send()


# ====================== Bot ======================
class MyTeamsBot(TeamsActivityHandler):
    async def on_turn(self, turn_context: TurnContext):
        logger.info(
            "on_turn activity type=%s name=%s text=%s value=%s channel_id=%s",
            turn_context.activity.type,
            getattr(turn_context.activity, "name", None),
            getattr(turn_context.activity, "text", None),
            safe_json_dumps(getattr(turn_context.activity, "value", None)),
            getattr(turn_context.activity, "channel_id", None),
        )

        try:
            await get_bot_data(turn_context)
            turn_context.on_send_activities(log_outgoing_activities)
            await super().on_turn(turn_context)
            await conversation_state.save_changes(turn_context, force=True)
        except Exception:
            logger.exception("Error in on_turn")
            raise

    async def on_teams_signin_token_exchange_activity(self, turn_context: TurnContext):
        logger.info(
            "signin/tokenExchange received. value=%s",
            safe_json_dumps(turn_context.activity.value),
        )

        teams_user_id = turn_context.activity.from_property.id
        value = turn_context.activity.value or {}
        token = value.get("token")

        if not token:
            logger.warning(
                "signin/tokenExchange received but no token in value: %s",
                safe_json_dumps(value),
            )
            await turn_context.send_activity(
                Activity(
                    type="invokeResponse",
                    value={"status": 412},
                )
            )
            return

        logger.info("Silent SSO token exchange succeeded for user=%s", teams_user_id)

        try:
            claims = decode_token_claims(token)
            logger.info("SSO token claims=%s", safe_json_dumps(claims))
        except Exception:
            logger.exception("Could not decode SSO token")

        USER_TOKEN_STORE[teams_user_id] = token

        await turn_context.send_activity(
            Activity(
                type="invokeResponse",
                value={"status": 200},
            )
        )

        name = None
        try:
            claims = decode_token_claims(token)
            name = claims.get("name") or claims.get("preferred_username")
        except Exception:
            logger.exception("Failed to read token claims for greeting")

        await turn_context.send_activity(
            f"✅ You're signed in{f' as **{name}**' if name else ''}. Ask me anything!"
        )

        data = await get_bot_data(turn_context)
        pending_message = data.get("pending_message")
        if pending_message:
            logger.info("Replaying pending message after sign-in")
            data.pop("pending_message", None)
            await bot_data_accessor.set(turn_context, data)
            await send_databricks_response(turn_context, token, pending_message)

    async def on_teams_signin_verify_state_activity(self, turn_context: TurnContext):
        logger.info(
            "signin/verifyState received. value=%s",
            safe_json_dumps(turn_context.activity.value),
        )
        await turn_context.send_activity(
            Activity(type="invokeResponse", value={"status": 200})
        )

    async def on_sign_in_invoke(self, turn_context: TurnContext):
        logger.info(
            "Generic sign-in invoke received. name=%s value=%s",
            getattr(turn_context.activity, "name", None),
            safe_json_dumps(turn_context.activity.value),
        )
        return await super().on_sign_in_invoke(turn_context)

    async def on_invoke_activity(self, turn_context: TurnContext):
        logger.info(
            "on_invoke_activity received. name=%s value=%s",
            getattr(turn_context.activity, "name", None),
            safe_json_dumps(turn_context.activity.value),
        )
        return await super().on_invoke_activity(turn_context)

    async def on_message_activity(self, turn_context: TurnContext):
        try:
            if turn_context.activity.value:
                logger.info(
                    "Card submit received in on_message_activity. value=%s",
                    safe_json_dumps(turn_context.activity.value),
                )
                await self.handle_card_submit(turn_context)
                return

            user_text_raw = (turn_context.activity.text or "").strip()
            user_text = user_text_raw.lower()

            if not user_text_raw:
                logger.info("Empty message received. Ignoring.")
                return

            data = await get_bot_data(turn_context)
            if "model" not in data:
                data["model"] = "fast"

            teams_user_id = turn_context.activity.from_property.id
            conversation_id = turn_context.activity.conversation.id

            logger.info(
                "Message received. user_id=%s conversation_id=%s text=%s",
                teams_user_id,
                conversation_id,
                user_text_raw,
            )

            if user_text == "logout":
                USER_TOKEN_STORE.pop(teams_user_id, None)
                data["is_logged_in"] = False
                await bot_data_accessor.set(turn_context, data)
                await turn_context.send_activity("You have been signed out.")
                logger.info("User logged out. user_id=%s", teams_user_id)
                return

            if user_text == "whoami":
                token = USER_TOKEN_STORE.get(teams_user_id)
                if not token:
                    logger.info("No token found for whoami. Sending sign-in card.")
                    await turn_context.send_activity(
                        "No user token found. Sending sign-in card…"
                    )
                    await turn_context.send_activity(
                        MessageFactory.attachment(build_signin_card())
                    )
                    return

                try:
                    claims = decode_token_claims(token)
                    await turn_context.send_activity(
                        f"Token claims:\n```json\n{json.dumps(claims, indent=2)}\n```"
                    )
                except Exception as exc:
                    logger.exception("Could not decode token in whoami")
                    await turn_context.send_activity(
                        f"Could not decode token: {str(exc)}"
                    )
                return

            if user_text in {"select model", "change model", "model"}:
                logger.info("Sending model selector card")
                card = build_model_dropdown_card(data.get("model", "fast"))
                await turn_context.send_activity(
                    MessageFactory.attachment(CardFactory.adaptive_card(card))
                )
                await bot_data_accessor.set(turn_context, data)
                return

            token = USER_TOKEN_STORE.get(teams_user_id)
            if not token:
                logger.info(
                    "No token for user=%s. Storing pending message and sending OAuth card.",
                    teams_user_id,
                )
                data["pending_message"] = user_text_raw
                await bot_data_accessor.set(turn_context, data)

                await turn_context.send_activity(
                    MessageFactory.attachment(build_signin_card())
                )
                return

            logger.info(
                "User authenticated. Sending question to Databricks. model=%s text_len=%s",
                data.get("model"),
                len(user_text_raw),
            )
            await send_databricks_response(turn_context, token, user_text_raw)

        except Exception as exc:
            logger.exception("Error in on_message_activity")
            await turn_context.send_activity(f"❌ Bot error: {str(exc)}")

    async def handle_card_submit(self, turn_context: TurnContext):
        try:
            payload = turn_context.activity.value or {}
            action = payload.get("action")
            state = await get_bot_data(turn_context)

            logger.info("Handling card submit. action=%s payload=%s", action, safe_json_dumps(payload))

            if "model" not in state:
                state["model"] = "fast"

            if action == "setModel":
                selected_model = (payload.get("model") or "fast").strip().lower()
                if selected_model not in {"fast", "thinking", "pro", "ultra"}:
                    selected_model = "fast"

                state["model"] = selected_model
                await bot_data_accessor.set(turn_context, state)
                await turn_context.send_activity(
                    f"Model updated to **{selected_model}**."
                )
                logger.info("Model updated to %s", selected_model)
                return

            if action in {"feedback_up", "feedback_down"}:
                feedback_id = payload.get("feedback_id")
                item = feedback_store.get(feedback_id)
                if item:
                    item["feedback"] = "up" if action == "feedback_up" else "down"
                    item["feedback_at"] = utcnow().isoformat()
                    logger.info(
                        "Feedback stored. feedback_id=%s feedback=%s",
                        feedback_id,
                        item["feedback"],
                    )
                else:
                    logger.warning("Feedback id not found: %s", feedback_id)

                await turn_context.send_activity("Thanks for the feedback.")
                return

            await turn_context.send_activity("Action received.")
            logger.info("Unknown card action handled as generic action")

        except Exception as exc:
            logger.exception("Error in handle_card_submit")
            await turn_context.send_activity(f"❌ Error handling card: {str(exc)}")


bot = MyTeamsBot()


# ====================== Function Entry Point ======================
@app.route(route="messages", methods=["POST"])
async def messages(req: func.HttpRequest) -> func.HttpResponse:
    try:
        body_bytes = req.get_body()
        activity_json = json.loads(body_bytes.decode("utf-8"))
        activity = Activity().deserialize(activity_json)
        auth_header = req.headers.get("Authorization", "")

        logger.info("Raw incoming activity json=%s", safe_json_dumps(activity_json))
        logger.info(
            "Incoming activity type=%s name=%s channel=%s from=%s",
            activity.type,
            getattr(activity, "name", None),
            activity.channel_id,
            getattr(activity.from_property, "id", None),
        )

        invoke_response = await adapter.process_activity(
            activity,
            auth_header,
            bot.on_turn,
        )

        if invoke_response:
            logger.info(
                "Returning invoke response. status=%s body=%s",
                invoke_response.status,
                safe_json_dumps(invoke_response.body),
            )
            return func.HttpResponse(
                body=json.dumps(invoke_response.body),
                status_code=invoke_response.status,
                mimetype="application/json",
            )

        logger.info("Returning HTTP 200 with empty body")
        return func.HttpResponse(status_code=200)

    except Exception as exc:
        logger.exception("Error in messages endpoint")
        return func.HttpResponse(f"Error: {str(exc)}", status_code=500)
