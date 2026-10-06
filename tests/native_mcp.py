"""Exercise the real native SDK MCP transport without a model or network."""
import anyio
from mcp import ClientSession
from mcp.shared.memory import create_client_server_memory_streams

async def call_mcp(config, name, arguments):
    server = config["instance"]
    async with create_client_server_memory_streams() as (client_streams, server_streams):
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(server.run, *server_streams, server.create_initialization_options())
            async with ClientSession(*client_streams) as session:
                await session.initialize()
                result = await session.call_tool(name, arguments)
            tasks.cancel_scope.cancel()
    return result.model_dump(by_alias=True)
