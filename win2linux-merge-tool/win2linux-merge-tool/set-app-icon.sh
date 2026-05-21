#!/bin/bash
# set-app-icon.sh
# Script to set the icon on the macOS app bundle

APP_PATH="${1:-dist/win2linux-merge-tool-web.app}"
ICON_PATH="${2:-assets/icon.icns}"

if [ ! -d "$APP_PATH" ]; then
    echo "Error: App bundle not found: $APP_PATH"
    exit 1
fi

if [ ! -f "$ICON_PATH" ]; then
    echo "Error: Icon file not found: $ICON_PATH"
    exit 1
fi

RESOURCES_PATH="$APP_PATH/Contents/Resources"
if [ ! -d "$RESOURCES_PATH" ]; then
    echo "Error: Resources directory not found in app bundle"
    exit 1
fi

# Copy icon to Resources if not already there
if [ ! -f "$RESOURCES_PATH/icon.icns" ]; then
    cp "$ICON_PATH" "$RESOURCES_PATH/icon.icns"
fi

echo "Setting icon on: $APP_PATH"

# Method 1: Try fileicon (best method)
if command -v fileicon >/dev/null 2>&1; then
    if fileicon set "$APP_PATH" "$RESOURCES_PATH/icon.icns"; then
        echo "✓ Icon set successfully using fileicon"
        killall Finder 2>/dev/null || true
        exit 0
    fi
fi

# Method 2: Try Python with PyObjC
python3 << PYTHON_EOF 2>/dev/null
import sys
try:
    from AppKit import NSWorkspace, NSImage
    workspace = NSWorkspace.sharedWorkspace()
    icon_image = NSImage.alloc().initWithContentsOfFile_("$RESOURCES_PATH/icon.icns")
    if icon_image:
        workspace.setIcon_forFile_options_(icon_image, "$APP_PATH", 0)
        print("✓ Icon set successfully using PyObjC")
        sys.exit(0)
except:
    sys.exit(1)
PYTHON_EOF

if [ $? -eq 0 ]; then
    killall Finder 2>/dev/null || true
    exit 0
fi

# Method 3: Manual instructions
echo ""
echo "Automatic icon setting failed. To set the icon manually:"
echo "  1. Right-click on: $APP_PATH"
echo "  2. Select 'Get Info'"
echo "  3. Drag $RESOURCES_PATH/icon.icns onto the small icon in the top-left of the Get Info window"
echo "  4. The icon should update immediately"
echo ""
echo "Or install fileicon: brew install fileicon"
echo "Then run: fileicon set '$APP_PATH' '$RESOURCES_PATH/icon.icns'"
