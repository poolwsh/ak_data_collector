@echo off
powershell -NoProfile -ExecutionPolicy Bypass -Command "& {.\convert_line_endings.ps1; Read-Host 'Press Enter to exit'}"
