import subprocess
def clean_tmp():
    # subprocess.run(['rm', '-rf', './tmp'])

    # Вариант 3: с проверкой результата
    result = subprocess.run(['rm', '-rf', './tmp'], capture_output=True, text=True)
    if result.returncode == 0:
        print("Успешно удалено")
    else:
        print(f"Ошибка: {result.stderr}")