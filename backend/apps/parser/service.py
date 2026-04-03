"""解析任务执行服务。"""

import asyncio

from apps.parser.call_llm import executor_llm
from apps.parser.pdf_parser import pdf_parser
from apps.parser.state import (
    TaskStatus,
    cleanup_task,
    update_task_error,
    update_task_result,
    update_task_status,
)
from shared.exceptions.base import ParseError


def infer_parser_type(filename: str, content_type: str | None) -> str:
    """根据文件名和内容类型推断解析器类型。

    Args:
        filename: 上传文件的文件名。
        content_type: 文件的 MIME 类型。

    Returns:
        解析器类型标识符，如 "pdf"。

    Raises:
        ValueError: 不支持的文件类型。
    """
    ext = filename.lower().split(".")[-1] if "." in filename else ""

    if ext == "pdf" or content_type == "application/pdf":
        return "pdf"

    raise ValueError(f"不支持的文件类型: {ext or content_type or '未知'}")


async def _execute_parse_flow(
    task_id: str,
    file_path: str,
    base_url: str,
    api_key: str,
    model: str,
    *,
    delete_file: bool,
) -> None:
    """执行解析流程的公共逻辑。

    编排整个解析流程：PDF 文本提取 -> LLM 解析。

    Args:
        task_id: 任务 ID。
        file_path: PDF 文件绝对路径。
        base_url: AI API 地址。
        api_key: AI API 密钥。
        model: 模型名称。
        delete_file: 是否在清理时删除上传文件。
    """
    try:
        await update_task_status(task_id, TaskStatus.RUNNING)
        # TODO(java): 上报任务状态为 RUNNING
        # await java_client.patch(
        #     endpoints.get_task_update_url(task_id),
        #     json={"status": TaskStatus.RUNNING.value},
        # )

        result = await pdf_parser.parse(file_path)
        result = await executor_llm(api_key, base_url, model, result["text"])

        await update_task_result(task_id, result.model_dump())
        # TODO(java): 上报任务状态为 SUCCESS
        # await java_client.patch(
        #     endpoints.get_task_update_url(task_id),
        #     json={"status": TaskStatus.SUCCESS.value},
        # )
        asyncio.create_task(cleanup_task(task_id, file_path, delete_file=delete_file))

    except ParseError as e:
        await update_task_error(task_id, str(e))
        # TODO(java): 上报任务状态为 ERROR，错误信息为 e
        # await java_client.patch(
        #     endpoints.get_task_update_url(task_id),
        #     json={"status": TaskStatus.ERROR.value, "error": str(e)},
        # )
        asyncio.create_task(cleanup_task(task_id, file_path, delete_file=False))
    except Exception as e:
        await update_task_error(task_id, f"解析失败: {str(e)}")
        # TODO(java): 上报任务状态为 ERROR，错误信息为 f"解析失败: {str(e)}"
        # await java_client.patch(
        #     endpoints.get_task_update_url(task_id),
        #     json={"status": TaskStatus.ERROR.value, "error": f"解析失败: {str(e)}"},
        # )
        asyncio.create_task(cleanup_task(task_id, file_path, delete_file=False))


async def run_parser_task(
    task_id: str,
    file_path: str,
    base_url: str,
    api_key: str,
    model: str,
) -> None:
    """后台解析任务执行函数。

    编排整个解析流程：PDF 文本提取 -> LLM 解析。

    Args:
        task_id: 任务 ID。
        file_path: PDF 文件绝对路径。
        base_url: AI API 地址。
        api_key: AI API 密钥。
        model: 模型名称。
    """
    # TODO(java): 创建任务记录到 Java 后端
    # await java_client.post(
    #     endpoints.get_url(endpoints.task_create),
    #     json={
    #         "taskId": task_id,
    #         "filePath": file_path,
    #         "status": TaskStatus.PENDING.value,
    #     },
    # )
    await _execute_parse_flow(
        task_id, file_path, base_url, api_key, model, delete_file=True
    )


async def retry_parser_task(
    task_id: str,
    file_path: str,
    base_url: str,
    api_key: str,
    model: str,
) -> None:
    """重试解析任务执行函数。

    读取已提取的文本内容，重新调用 LLM 解析。

    Args:
        task_id: 任务 ID。
        file_path: PDF 文件绝对路径（暂未使用，保留接口兼容性）。
        base_url: AI API 地址。
        api_key: AI API 密钥。
        model: 模型名称。
    """
    # TODO(java): 从 Java 后端获取任务信息，校验状态为 ERROR
    # java_response = await java_client.get(endpoints.get_task_url(task_id))
    # task_info = java_response.json()
    # if task_info["status"] != TaskStatus.ERROR.value:
    #     raise HTTPException(status_code=400, detail="只有错误状态的任务才能重试")
    # file_path = task_info["filePath"]
    await _execute_parse_flow(
        task_id, file_path, base_url, api_key, model, delete_file=True
    )
