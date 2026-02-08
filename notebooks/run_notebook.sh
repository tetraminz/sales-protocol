#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NOTEBOOK_DIR="${ROOT_DIR}/notebooks"
ENV_FILE="${NOTEBOOK_DIR}/environment.yml"
ENV_NAME="sales-jupyter"
NOTEBOOK_PATH="${NOTEBOOK_DIR}/llm_quality_analysis_ru.ipynb"

ACTION="${1:-lab}"
PKG_MANAGER=""
UPDATE_ENV="${UPDATE_ENV:-0}"
PREFER_MICROMAMBA="${PREFER_MICROMAMBA:-0}"
FORCE_KERNEL_INSTALL="${FORCE_KERNEL_INSTALL:-0}"

usage() {
  cat <<'EOF'
Использование:
  notebooks/run_notebook.sh setup      # создать/обновить env и kernel
  notebooks/run_notebook.sh lab        # setup + открыть Jupyter Lab
  notebooks/run_notebook.sh notebook   # setup + открыть классический Notebook
  notebooks/run_notebook.sh check      # setup + выполнить ноутбук headless

Переменные окружения:
  UPDATE_ENV=1         # принудительно обновить уже существующий env
  PREFER_MICROMAMBA=1  # пробовать micromamba раньше conda
  FORCE_KERNEL_INSTALL=1  # принудительно переустановить Jupyter kernel
EOF
}

detect_pkg_manager() {
  # По умолчанию используем conda: на части систем micromamba может падать с Abort trap.
  if [[ "${PREFER_MICROMAMBA}" == "1" ]]; then
    if command -v micromamba >/dev/null 2>&1; then
      PKG_MANAGER="micromamba"
      return
    fi
    if command -v conda >/dev/null 2>&1; then
      PKG_MANAGER="conda"
      return
    fi
  else
    if command -v conda >/dev/null 2>&1; then
      PKG_MANAGER="conda"
      return
    fi
    if command -v micromamba >/dev/null 2>&1; then
      PKG_MANAGER="micromamba"
      return
    fi
  fi

  echo "Не найден рабочий пакетный менеджер. Нужен micromamba или conda." >&2
  exit 1
}

env_exists() {
  if [[ "${PKG_MANAGER}" == "micromamba" ]]; then
    micromamba env list | awk 'NR > 1 {print $1}' | grep -qx "${ENV_NAME}"
    return
  fi

  conda env list | awk 'NR > 2 && $1 !~ /^#/ {print $1}' | grep -qx "${ENV_NAME}"
}

run_in_env() {
  if [[ "${PKG_MANAGER}" == "micromamba" ]]; then
    micromamba run -n "${ENV_NAME}" "$@"
    return
  fi

  conda run -n "${ENV_NAME}" "$@"
}

ensure_env() {
  if env_exists; then
    echo "Окружение ${ENV_NAME} уже существует."
    if [[ "${UPDATE_ENV}" == "1" ]]; then
      echo "Принудительно обновляю окружение ${ENV_NAME} по ${ENV_FILE}..."
      if [[ "${PKG_MANAGER}" == "micromamba" ]]; then
        micromamba env update -n "${ENV_NAME}" -f "${ENV_FILE}"
      else
        conda env update -n "${ENV_NAME}" -f "${ENV_FILE}" --prune
      fi
    else
      echo "Обновление пропущено (UPDATE_ENV=0)."
    fi
  else
    echo "Создаю окружение ${ENV_NAME} через ${PKG_MANAGER}..."
    if [[ "${PKG_MANAGER}" == "micromamba" ]]; then
      micromamba create -y -f "${ENV_FILE}"
    else
      conda env create -y -f "${ENV_FILE}"
    fi
  fi
}

register_kernel() {
  local kernel_dir_macos="${HOME}/Library/Jupyter/kernels/${ENV_NAME}"
  local kernel_dir_linux="${HOME}/.local/share/jupyter/kernels/${ENV_NAME}"

  if [[ "${FORCE_KERNEL_INSTALL}" != "1" ]] && [[ -d "${kernel_dir_macos}" || -d "${kernel_dir_linux}" ]]; then
    echo "Kernel ${ENV_NAME} уже зарегистрирован. Переустановка пропущена."
    return
  fi

  echo "Регистрирую kernel ${ENV_NAME}..."
  if ! run_in_env python -m ipykernel install --user --name "${ENV_NAME}" --display-name "Python (${ENV_NAME})"; then
    if [[ -d "${kernel_dir_macos}" || -d "${kernel_dir_linux}" ]]; then
      echo "Предупреждение: не удалось переустановить kernel, использую существующий."
      return
    fi
    echo "Ошибка: не удалось зарегистрировать kernel ${ENV_NAME}." >&2
    exit 1
  fi
}

setup() {
  detect_pkg_manager
  echo "Пакетный менеджер: ${PKG_MANAGER}"
  ensure_env
  register_kernel
}

case "${ACTION}" in
  setup)
    setup
    ;;
  lab)
    setup
    run_in_env jupyter lab "${NOTEBOOK_PATH}"
    ;;
  notebook)
    setup
    run_in_env jupyter notebook "${NOTEBOOK_PATH}"
    ;;
  check)
    setup
    run_in_env jupyter nbconvert \
      --to notebook \
      --execute \
      --inplace \
      --ExecutePreprocessor.timeout=300 \
      "${NOTEBOOK_PATH}"
    ;;
  *)
    usage
    exit 2
    ;;
esac
