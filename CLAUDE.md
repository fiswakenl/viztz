# CLAUDE.md

**Используй uv для Python:**
```bash
pip install → uv add
pip uninstall → uv remove
python script.py → uv run python script.py
pytest → uv run pytest
```

**После генерации кода:**
```bash
uv run ruff check --fix .
```

**MCP локально:**
```bash
claude mcp add <name> --scope project -- uvx <server>
```

**Автоматическое использование MCP:**
- Планирование/архитектура → Sequential Thinking
- Веб-скрапинг → Brightdata
- Разбивай сложные задачи на шаги через MCP

**Правила:**
- Не используй эмодзи
- Всегда `uv run` для команд проекта
- Управляй зависимостями только через `uv add/remove`, не редактируй pyproject.toml вручную