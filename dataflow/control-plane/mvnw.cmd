@echo off
set SCRIPT_DIR=%~dp0
java -jar "%SCRIPT_DIR%.mvn\wrapper\maven-wrapper.jar" -Dmaven.multiModuleProjectDirectory=%SCRIPT_DIR% %*
