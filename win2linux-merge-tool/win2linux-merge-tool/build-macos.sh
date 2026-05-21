#!/bin/bash
# build-macos.sh
# Build script for macOS PyInstaller bundle

set -e  # Exit on error

# Parse command line arguments
CLEAN_BUILD=false
while [[ $# -gt 0 ]]; do
    case $1 in
        --clean)
            CLEAN_BUILD=true
            shift
            ;;
        -h|--help)
            echo "Usage: ./build-macos.sh [--clean]"
            echo ""
            echo "Options:"
            echo "  --clean    Clean previous build artifacts before building"
            echo "  -h, --help Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
ICON_PNG="assets/Sisense Icon Blue.png"
ICON_ICNS="assets/icon.icns"
SPEC_FILE="build-web-macos.spec"
VENV_DIR=".venv"
BUILD_DIR="build"
DIST_DIR="dist"

echo -e "${GREEN}=== macOS Build Script ===${NC}"
echo ""

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to convert PNG to ICNS
convert_png_to_icns() {
    local png_file=$1
    local icns_file=$2
    
    if [ ! -f "$png_file" ]; then
        echo -e "${YELLOW}Warning: PNG icon file not found: $png_file${NC}"
        return 1
    fi
    
    echo -e "${GREEN}Converting PNG to ICNS...${NC}"
    
    # Create temporary iconset directory
    local iconset_dir="assets/icon.iconset"
    rm -rf "$iconset_dir"
    mkdir -p "$iconset_dir"
    
    # Generate all required icon sizes for macOS
    # macOS requires multiple sizes in the iconset
    sizes=(16 32 64 128 256 512 1024)
    
    for size in "${sizes[@]}"; do
        # Create @1x and @2x versions
        sips -z $size $size "$png_file" --out "$iconset_dir/icon_${size}x${size}.png" >/dev/null 2>&1
        sips -z $((size * 2)) $((size * 2)) "$png_file" --out "$iconset_dir/icon_${size}x${size}@2x.png" >/dev/null 2>&1
    done
    
    # Convert iconset to icns
    if iconutil -c icns "$iconset_dir" -o "$icns_file" 2>/dev/null; then
        echo -e "${GREEN}✓ Successfully created $icns_file${NC}"
        rm -rf "$iconset_dir"
        return 0
    else
        echo -e "${YELLOW}Warning: iconutil failed, will use PNG instead${NC}"
        rm -rf "$iconset_dir"
        return 1
    fi
}

# Function to set icon using Python/PyObjC or alternative methods
set_icon_alternative() {
    local app_path="$APP_PATH"
    local icon_path="$RESOURCES_PATH/icon.icns"
    
    # Try using Python with PyObjC (if available)
    python3 << PYTHON_EOF 2>/dev/null
import sys
import os

app_path = "$app_path"
icon_path = "$icon_path"

try:
    from AppKit import NSWorkspace, NSImage
    workspace = NSWorkspace.sharedWorkspace()
    icon_image = NSImage.alloc().initWithContentsOfFile_(icon_path)
    if icon_image:
        workspace.setIcon_forFile_options_(icon_image, app_path, 0)
        print("✓ Icon set using Cocoa/AppKit")
        sys.exit(0)
except ImportError:
    # PyObjC not available
    pass
except Exception as e:
    # Silently fail
    pass

sys.exit(1)
PYTHON_EOF
    
    return $?
}

# Function to create a proper .app bundle with icon
create_app_bundle() {
    APP_NAME="win2linux-merge-tool-web.app"
    APP_PATH="$DIST_DIR/$APP_NAME"
    CONTENTS_PATH="$APP_PATH/Contents"
    MACOS_PATH="$CONTENTS_PATH/MacOS"
    RESOURCES_PATH="$CONTENTS_PATH/Resources"
    ORIGINAL_BUILD="$DIST_DIR/win2linux-merge-tool-web"
    
    # Remove existing app bundle if it exists
    rm -rf "$APP_PATH"
    
    # Create app bundle structure
    mkdir -p "$MACOS_PATH"
    mkdir -p "$RESOURCES_PATH"
    FRAMEWORKS_PATH="$CONTENTS_PATH/Frameworks"
    mkdir -p "$FRAMEWORKS_PATH"
    
    # Copy the entire original build directory to MacOS
    # This preserves the PyInstaller structure (executable + _internal folder)
    cp -R "$ORIGINAL_BUILD/"* "$MACOS_PATH/"
    
    # PyInstaller bootloader looks for ALL application files in Frameworks when in app bundle
    # Copy the entire _internal directory contents to Frameworks
    # This is necessary because PyInstaller's bootloader changes search paths in app bundles
    if [ -d "$MACOS_PATH/_internal" ]; then
        # Copy everything from _internal to Frameworks
        cp -R "$MACOS_PATH/_internal/"* "$FRAMEWORKS_PATH/"
        echo -e "${GREEN}✓ Copied all application files to Frameworks${NC}"
    fi
    
    
    # Ensure executable has proper permissions
    chmod +x "$MACOS_PATH/win2linux-merge-tool-web"
    
    # Note: The executable should work from MacOS directory
    # PyInstaller bootloader will look for _internal in the same directory as the executable
    
    # Copy icon to Resources
    cp "$ICON_ICNS" "$RESOURCES_PATH/icon.icns"
    
    # Create Info.plist
    cat > "$CONTENTS_PATH/Info.plist" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleExecutable</key>
    <string>win2linux-merge-tool-web</string>
    <key>CFBundleIconFile</key>
    <string>icon</string>
    <key>CFBundleIdentifier</key>
    <string>com.sisense.win2linux-merge-tool</string>
    <key>CFBundleName</key>
    <string>Win2Linux Merge Tool</string>
    <key>CFBundlePackageType</key>
    <string>APPL</string>
    <key>CFBundleVersion</key>
    <string>1.0</string>
    <key>LSMinimumSystemVersion</key>
    <string>10.13</string>
    <key>NSHighResolutionCapable</key>
    <true/>
    <key>NSRequiresAquaSystemAppearance</key>
    <false/>
</dict>
</plist>
EOF
    
    # Set the icon on the app bundle using macOS tools
    # This ensures Finder displays the icon
    echo -e "${GREEN}[Post-build] Setting icon on app bundle...${NC}"
    
    # Method 1: Try using fileicon (install if not available)
    ICON_SET=false
    if ! command_exists fileicon; then
        echo -e "${YELLOW}fileicon not found. Attempting to install via Homebrew...${NC}"
        if command_exists brew; then
            if brew install fileicon 2>/dev/null; then
                echo -e "${GREEN}✓ Installed fileicon${NC}"
            else
                echo -e "${YELLOW}Warning: Could not install fileicon. You may need to set the icon manually.${NC}"
            fi
        else
            echo -e "${YELLOW}Warning: Homebrew not found. Install fileicon manually: brew install fileicon${NC}"
        fi
    fi
    
    if command_exists fileicon; then
        if fileicon set "$APP_PATH" "$RESOURCES_PATH/icon.icns" 2>/dev/null; then
            echo -e "${GREEN}✓ Icon set using fileicon${NC}"
            ICON_SET=true
        else
            echo -e "${YELLOW}Warning: fileicon failed to set icon${NC}"
        fi
    fi
    
    # Method 2: Use Python with PyObjC to set icon
    if [ "$ICON_SET" = false ] && command_exists python3; then
        if set_icon_alternative; then
            ICON_SET=true
        fi
    fi
    
    # Method 3: Use sips and DeRez/Rez to set icon resource (fallback)
    if [ "$ICON_SET" = false ] && command_exists sips && command_exists DeRez && command_exists Rez; then
        echo -e "${YELLOW}Trying to set icon using sips/DeRez/Rez...${NC}"
        # This is complex, so we'll skip it for now
    fi
    
    # Touch the app bundle and icon file to trigger refresh
    touch "$APP_PATH"
    touch "$RESOURCES_PATH/icon.icns"
    
    # Refresh Finder's icon cache
    if command_exists killall; then
        echo -e "${GREEN}Refreshing Finder icon cache...${NC}"
        killall Finder 2>/dev/null || true
        sleep 1
        echo -e "${GREEN}✓ Finder refreshed${NC}"
    fi
    
    if [ "$ICON_SET" = false ]; then
        echo -e "${YELLOW}Note: Icon may not display immediately.${NC}"
        echo -e "${YELLOW}To set the icon, run: ./set-app-icon.sh${NC}"
        echo -e "${YELLOW}Or manually: Get Info on the app, drag icon.icns onto the icon in Get Info${NC}"
    fi
    
    echo -e "${GREEN}✓ Created .app bundle: $APP_PATH${NC}"
    echo -e "${GREEN}  The icon should now display in Finder${NC}"
    echo ""
    echo -e "${GREEN}Portability Note:${NC}"
    echo -e "  The icon is set via Info.plist (CFBundleIconFile), which is portable."
    echo -e "  When deploying to another Mac, the icon will display correctly even"
    echo -e "  if fileicon is not installed on that system."
    echo ""
    echo "To run the application:"
    echo "  open $APP_PATH"
    echo "  or"
    echo "  $APP_PATH/Contents/MacOS/win2linux-merge-tool-web"
}

# Step 1: Check prerequisites
echo -e "${GREEN}[1/6] Checking prerequisites...${NC}"
if ! command_exists python3; then
    echo -e "${RED}Error: python3 not found${NC}"
    exit 1
fi

if ! command_exists uv; then
    echo -e "${RED}Error: uv not found. Please install uv: curl -LsSf https://astral.sh/uv/install.sh | sh${NC}"
    exit 1
fi

# Step 2: Handle icon conversion
echo -e "${GREEN}[2/6] Handling icon...${NC}"
if [ ! -f "$ICON_ICNS" ]; then
    if command_exists sips && command_exists iconutil; then
        convert_png_to_icns "$ICON_PNG" "$ICON_ICNS"
        USE_ICNS=true
    else
        echo -e "${YELLOW}Warning: sips/iconutil not available, will use PNG icon${NC}"
        USE_ICNS=false
    fi
else
    echo -e "${GREEN}✓ ICNS icon already exists${NC}"
    USE_ICNS=true
fi

# Step 3: Update spec file with correct icon
echo -e "${GREEN}[3/6] Updating spec file with icon path...${NC}"
if [ "$USE_ICNS" = true ] && [ -f "$ICON_ICNS" ]; then
    # Use sed to update the icon path in the spec file
    if [[ "$OSTYPE" == "darwin"* ]]; then
        sed -i '' "s|icon='.*'|icon='$ICON_ICNS'|" "$SPEC_FILE"
    else
        sed -i "s|icon='.*'|icon='$ICON_ICNS'|" "$SPEC_FILE"
    fi
    echo -e "${GREEN}✓ Updated spec file to use $ICON_ICNS${NC}"
else
    echo -e "${GREEN}✓ Using PNG icon in spec file${NC}"
fi

# Step 4: Set up virtual environment
echo -e "${GREEN}[4/6] Setting up virtual environment...${NC}"
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment..."
    uv venv
else
    echo "Virtual environment already exists"
fi

# Activate virtual environment
source "$VENV_DIR/bin/activate"

# Step 5: Install dependencies
echo -e "${GREEN}[5/6] Installing dependencies...${NC}"
uv pip install -r requirements.txt

# Ensure PyInstaller is installed
if ! python -m PyInstaller --version >/dev/null 2>&1; then
    echo "Installing PyInstaller..."
    uv pip install pyinstaller
fi

# Step 6: Clean previous builds (if requested)
if [ "$CLEAN_BUILD" = true ]; then
    echo -e "${YELLOW}Cleaning previous builds...${NC}"
    rm -rf "$BUILD_DIR/build-web-macos"
    rm -rf "$DIST_DIR/win2linux-merge-tool-web"
fi

# Step 7: Build with PyInstaller
echo -e "${GREEN}[6/6] Building with PyInstaller...${NC}"
echo ""
pyinstaller "$SPEC_FILE"

# Check if build was successful
if [ -d "$DIST_DIR/win2linux-merge-tool-web" ]; then
    EXECUTABLE="$DIST_DIR/win2linux-merge-tool-web/win2linux-merge-tool-web"
    
    # Step 8: Create .app bundle with icon (macOS-specific)
    if [ -f "$EXECUTABLE" ] && [ -f "$ICON_ICNS" ]; then
        echo -e "${GREEN}[Post-build] Creating .app bundle with icon...${NC}"
        create_app_bundle
    fi
    
    echo ""
    echo -e "${GREEN}=== Build Complete! ===${NC}"
    echo -e "${GREEN}Output location: $DIST_DIR/win2linux-merge-tool-web/${NC}"
    echo ""
    echo "To run the application:"
    echo "  $EXECUTABLE"
    echo ""
    
    # Show size of built application
    if command_exists du; then
        SIZE=$(du -sh "$DIST_DIR/win2linux-merge-tool-web" | cut -f1)
        echo -e "Build size: ${GREEN}$SIZE${NC}"
    fi
else
    echo -e "${RED}Build failed! Check the output above for errors.${NC}"
    exit 1
fi
