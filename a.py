import os

def rename_files_and_directories(root_dir):
    for root, dirs, files in os.walk(root_dir, topdown=False):
        # 先重命名文件
        for name in files:
            if name.startswith('ak_da_'):
                old_path = os.path.join(root, name)
                new_path = os.path.join(root, 'da_ak_' + name[6:])
                os.rename(old_path, new_path)
                print(f"Renamed file: {old_path} -> {new_path}")
        
        # 再重命名目录
        for name in dirs:
            if name.startswith('ak_da_'):
                old_path = os.path.join(root, name)
                new_path = os.path.join(root, 'da_ak_' + name[6:])
                os.rename(old_path, new_path)
                print(f"Renamed directory: {old_path} -> {new_path}")

if __name__ == "__main__":
    # 修改这里的 '.' 为你想要开始遍历的根目录
    rename_files_and_directories('.')
