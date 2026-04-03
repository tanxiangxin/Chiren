import asyncio
import json

import json_repair
from openai import APIError, AsyncOpenAI, RateLimitError

from .prompt import CONTENT, SYSTEM
from .schemas import ParserResult

MAX_RETRIES = 5


def _is_retriable_error(exc: Exception) -> bool:
    return isinstance(exc, (RateLimitError, APIError))


async def executor_llm(
    api_key: str, base_url: str, model: str, content: str
) -> ParserResult:
    async with AsyncOpenAI(api_key=api_key, base_url=base_url) as client:
        last_exc: Exception | None = None
        for attempt in range(MAX_RETRIES + 1):
            try:
                res = await client.chat.completions.create(
                    model=model,
                    messages=[
                        {
                            "role": "system",
                            "content": SYSTEM.format(
                                json_schema=json.dumps(
                                    ParserResult.model_json_schema(),
                                    indent=2,
                                    ensure_ascii=False,
                                )
                            ),
                        },
                        {"role": "user", "content": CONTENT.format(content=content)},
                    ],
                )

                if res.choices is None or len(res.choices) == 0:
                    raise ValueError("No choices returned")

                choice = res.choices[0]

                if choice.message.content is None:
                    raise ValueError("No content returned")

                if not choice.message.content:
                    return ParserResult()

                json_data = json_repair.loads(choice.message.content)
                return ParserResult.model_validate(json_data)

            except ValueError as e:
                raise
            except Exception as exc:
                last_exc = exc
                if _is_retriable_error(exc) and attempt < MAX_RETRIES:
                    wait_seconds = 2**attempt
                    await asyncio.sleep(wait_seconds)
                    continue
                raise

        if last_exc:
            raise last_exc
        raise RuntimeError("Unexpected error")
