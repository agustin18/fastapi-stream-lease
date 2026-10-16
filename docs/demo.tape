Output docs/assets/demo.gif
Set FontSize 14
Set Width 860
Set Height 440
Set Padding 16
Set TypingSpeed 20ms

# 1. Start stream #1 in background (holds the 1 allowed user slot)
Type "# 1. Start active LLM stream in background (max_per_user=1)" Enter
Type "curl -sN -H 'X-API-Key: secret' http://localhost:8000/v1/chat/stream > /dev/null &" Sleep 100ms Enter
Sleep 300ms

# 2. Attempt concurrent stream #2 -> rejected before calling LLM
Type "# 2. Concurrent stream attempt is rejected before billing LLM tokens:" Enter
Type "curl -i -s -H 'X-API-Key: secret' http://localhost:8000/v1/chat/stream" Sleep 100ms Enter
Sleep 2s

# 3. Disconnect stream #1 -> Redis slot freed immediately
Type "# 3. Client 1 disconnects -> Redis slot freed immediately:" Enter
Type "kill %1 && sleep 0.2" Enter
Type "curl -i -N -H 'X-API-Key: secret' http://localhost:8000/v1/chat/stream" Sleep 100ms Enter
Sleep 2s
