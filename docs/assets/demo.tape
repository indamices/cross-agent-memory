# Regenerate the README demo GIF:
#   vhs docs/assets/demo.tape
Output docs/assets/demo.gif
Set Shell "zsh"
Set FontFamily "Menlo"
Set FontSize 15
Set Width 1000
Set Height 620
Set Padding 20
Set Theme "Catppuccin Mocha"
Set TypingSpeed 22ms
Set CursorBlink false

Sleep 500ms
Type "echo 'Cross-Agent Memory — one local memory for every AI agent'"
Enter
Sleep 600ms
Type "./install.sh"
Enter
Sleep 4200ms
Type "./.venv/bin/python examples/run.py decisions"
Enter
Sleep 3800ms
Type "MEMSVC_DB=/tmp/memsvc-gif/memory.db MEMSVC_PORT=5398 ./.venv/bin/memory-service >/tmp/memsvc-gif/server.log 2>&1 &"
Enter
Sleep 2200ms
Type "curl -s localhost:5398/api/ping | tr -d '\n '"
Enter
Sleep 1200ms
Type "/tmp/demo-search.sh"
Enter
Sleep 3200ms
Type "echo '→ MCP: memory_search · memory_write · memory_stats'"
Enter
Sleep 2200ms
