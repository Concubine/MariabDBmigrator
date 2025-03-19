# -*- mode: python ; coding: utf-8 -*-

import os

a = Analysis(
    ['C:\\Users\\plotk\\aihelp\\mariadbexport\\MDB2\\src\\main.py'],
    pathex=['C:\\Users\\plotk\\aihelp\\mariadbexport\\MDB2'],
    binaries=[],
    datas=[
        ('config/config.yaml', 'config'),
    ],
    hiddenimports=['src', 'src.core.config', 'src.core.exceptions', 'src.domain.models', 'src.infrastructure.mariadb', 'src.infrastructure.storage', 'src.services.export', 'src.services.import_', 'yaml', 'mysql.connector', 'mysql.connector.plugins', 'mysql.connector.plugins.mysql_native_password', 'sqlparse'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='mariadbexport',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

# Ensure config directory exists in dist
os.makedirs('dist/config', exist_ok=True)
