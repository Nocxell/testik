import os
import uuid
from multiprocessing import Pool, cpu_count
from concurrent.futures import ThreadPoolExecutor

def create_json_file(directory):
    """
    Создает один JSON файл с уникальным именем в указанной директории.
    
    :param directory: Путь к папке, где будет создан файл.
    """
    try:
        filename = f"fba_{uuid.uuid4().int}_{uuid.uuid4().int}.json"
        file_path = os.path.join(directory, filename)
        
        with open(file_path, 'w') as f:
            f.write('{}')  # Пустой JSON
        
        return f"Файл {filename} создан в {directory}"
    except Exception as e:
        return f"Ошибка при создании файла в {directory}: {e}"

def worker(directories, files_per_dir):
    """
    Рабочий процесс, который создаёт файлы в каждой указанной директории.
    
    :param directories: Список директорий, где будут созданы файлы.
    :param files_per_dir: Количество файлов, создаваемых в каждой директории.
    """
    results = []
    with ThreadPoolExecutor() as executor:
        for directory in directories:
            tasks = [executor.submit(create_json_file, directory) for _ in range(files_per_dir)]
            for task in tasks:
                results.append(task.result())
    return results

def get_all_directories(base_path):
    """
    Рекурсивно обходит все папки на указанном диске.
    
    :param base_path: Базовый путь (например, C:\).
    :return: Список всех найденных директорий.
    """
    directories = []
    for root, dirs, _ in os.walk(base_path):
        for dir_name in dirs:
            directories.append(os.path.join(root, dir_name))
    return directories

def create_json_files_in_all_dirs(base_path, total_files=100000, threads_per_process=10):
    """
    Создает JSON файлы во всех папках на указанном диске.
    
    :param base_path: Базовый путь, где будут созданы файлы.
    :param total_files: Общее количество создаваемых файлов.
    :param threads_per_process: Количество потоков на процесс.
    """
    print("Сканирование директорий...")
    directories = get_all_directories(base_path)
    print(f"Найдено {len(directories)} директорий.")

    if not directories:
        print("Не найдено директорий для создания файлов.")
        return

    files_per_dir = max(1, total_files // len(directories))
    num_processes = cpu_count()
    chunk_size = max(1, len(directories) // num_processes)

    with Pool(processes=num_processes) as pool:
        tasks = [directories[i:i + chunk_size] for i in range(0, len(directories), chunk_size)]
        results = pool.starmap(worker, [(chunk, files_per_dir) for chunk in tasks])
        for result in results:
            for line in result:
                print(line)

if __name__ == "__main__":
    base_path = "C:\\"  # Корень диска C
    total_files = 100000000  # Общее количество файлов
    threads_per_process = 20  # Количество потоков на процесс

    create_json_files_in_all_dirs(base_path, total_files, threads_per_process)
