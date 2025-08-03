.PHONY: tag test build-docs ruff mcp-server

tag:
	@version=$$(grep '^version = ' pyproject.toml | sed 's/version = "\(.*\)"/\1/'); \
	echo "Creating tag v$$version"; \
	git tag "v$$version"; \
	git push origin "v$$version"

test:
	export SURREAL_URL="ws://localhost:8018/rpc" && export SURREAL_USER="root" && export SURREAL_PASSWORD="root" && export SURREAL_NAMESPACE="test" && export SURREAL_DATABASE="test"; \
	uv run pytest -v

ruff:
	ruff check . --fix

mcp-server:
	uv run surreal-mcp
