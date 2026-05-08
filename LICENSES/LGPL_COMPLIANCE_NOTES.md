# LGPL Compliance Notes

ZSend Wallet uses PySide6 / Qt under the LGPL option.

For release builds, keep these rules in mind:

- Do not statically link Qt/PySide6 into the wallet.
- Keep PySide6/Qt as replaceable bundled dynamic libraries inside the PyInstaller
  extraction payload.
- Ship the LGPL/GPL license texts from this folder with binary releases.
- Provide the corresponding ZSend Wallet source code for the released build.
- Do not block users from replacing the LGPL-covered PySide6/Qt libraries with
  compatible modified versions.

This file is a practical release checklist, not a replacement for legal advice.
