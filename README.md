## Building from source (Windows)

### 1. Install Go

1. Download the Windows installer from https://go.dev/dl/. Pick the file ending in `windows-amd64.msi` (any recent version works).
2. Run the installer with the default options. Go is installed in `C:\Program Files\Go` and added to your `PATH` automatically.
3. Close any open terminal, open a new Command Prompt (`Win` + `R` → `cmd`), and check the installation:
   ```
   go version
   ```
   You should see something like `go version go1.xx windows/amd64`. If the command is not recognized, restart your PC so the `PATH` change takes effect.

### 2. Get the source code

Either clone the repository (requires [Git for Windows](https://git-scm.com/download/win)):
```
git clone https://github.com/juliensaccareau/deej.git
cd deej
```
or download it as a ZIP (**Code → Download ZIP** on GitHub), extract it, and open a terminal in the extracted folder.

### 3. Build

From the repository root, run:
```
go build -o deej.exe -ldflags "-H=windowsgui -s -w" ./pkg/deej/cmd
```
The first build downloads the dependencies, which may take a minute or two. When the command finishes without printing anything, `deej.exe` has been created in the current folder.

> `-H=windowsgui` builds deej without a console window, so it runs in the system tray only. Remove that flag if you want to see the logs in a console while debugging.

### 4. Install

1. Quit any running deej instance (right-click the tray icon → **Quit**).
2. Copy `deej.exe` to your deej folder, next to your `config.yaml`.
3. Optionally, add the new setting to your `config.yaml`:
   ```yaml
   # if no data is received from the arduino for this many seconds, deej considers the
   # connection dead and automatically reconnects (set to 0 to disable this watchdog,
   # e.g. if your arduino sketch only sends data when a slider moves)
   serial_timeout: 5
   ```
   If this line is missing, deej uses a 5-second timeout by default.
4. Start `deej.exe`.
