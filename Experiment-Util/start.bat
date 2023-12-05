@echo off
title MultiTerminalScript

start "master1" cmd /k main.exe master 1
start "master2" cmd /k main.exe master 2
start "master3" cmd /k main.exe master 3
start "master4" cmd /k main.exe master 4
start "master5" cmd /k main.exe master 5
start "worker1" cmd /k main.exe worker 1
start "worker2" cmd /k main.exe worker 2
start "worker3" cmd /k main.exe worker 3
start "worker4" cmd /k main.exe worker 4
start "worker5" cmd /k main.exe worker 5
start "worker6" cmd /k main.exe worker 6
start "worker7" cmd /k main.exe worker 7
start "worker8" cmd /k main.exe worker 8
start "worker9" cmd /k main.exe worker 9
start "worker10" cmd /k main.exe worker 10