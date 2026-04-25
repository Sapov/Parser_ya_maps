import subprocess
def clean_tmp():
    # subprocess.run(['rm', '-rf', './tmp'])

    # Вариант 3: с проверкой результата
    # result = subprocess.run(['rm', '-rf', './tmp'], capture_output=True, text=True)
    result = subprocess.run('rm -rf /tmp', shell=True)

    if result.returncode == 0:
        print("Успешно удалено")
    else:
        print(f"Ошибка: {result.stderr}")

if __name__ == '__main__':
    clean_tmp()