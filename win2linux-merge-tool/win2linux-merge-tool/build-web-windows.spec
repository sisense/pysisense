# build-web-windows.spec

# -*- mode: python ; coding: utf-8 -*-

block_cipher = None

a = Analysis(['server.py'],
             pathex=['.'],  # Look for modules in the project root
             binaries=[],
             datas=[
                 # Core directories needed for the web app and migration script
                 ('frontend', 'frontend'),
                 ('assets', 'assets'),
                 ('utils', 'utils'),
                 ('migration', 'migration'),  # Add the entire migration package

                 # Individual Python files that are direct dependencies
                 ('sisense_migration_and_merge_tool.py', '.'),
                 ('config_loader.py', '.'),
                 ('MigrationReportClass.py', '.'),
                 ('SisenseRESTAPIClientClass.py', '.'),
             ],
             hiddenimports=[
                 # Dependencies for the migration script that PyInstaller might miss
                 'deepdiff',
                 'alive_progress',
                 'pymongo',
                 'colorama',
                 'flask',  # Good to be explicit for the server
                 'yaml',    # Good to be explicit for config files
                 'waitress'
             ],
             hookspath=[],
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher,
             noarchive=False)

pyz = PYZ(a.pure, a.zipped_data,
          cipher=block_cipher)

exe = EXE(pyz,
          a.scripts,
          [],
          exclude_binaries=True,
          name='win2linux-merge-tool-web',
          debug=False,
          bootloader_ignore_signals=False,
          strip=False,
          upx=True,
          console=True,  # Correctly set to True to show the server console window.
          icon='assets/icon.ico')

coll = COLLECT(exe,
               a.binaries,
               a.zipfiles,
               a.datas,
               strip=False,
               upx=True,
               upx_exclude=[],
               name='win2linux-merge-tool-web')