# WSL:Ubuntu-20.04

Install WSL & update to WSL 2. Open PowerShell as Administrator and run:

`dism.exe /online /enable-feature /featurename:Microsoft-Windows-Subsystem-Linux /all /norestart`

`dism.exe /online /enable-feature /featurename:VirtualMachinePlatform /all /norestart`

[Download](https://wslstorestorage.blob.core.windows.net/wslblob/wsl_update_x64.msi) & Install

`wsl — set-default-version 2`

Download & Install Linux distribution [Ubuntu 20.04 LTS]

Check danh sách các máy ảo đang chạy và verion WSL đang sử dụng

`wsl — list — verbose`

Nếu máy ảo đang chạy trên version WSL v1 thì set lại version cho nó:

`wsl — set-version <distribution name> <versionNumber>`