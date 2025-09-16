import os
import shutil
import subprocess

META_VERSION_PATH = os.path.join('meta', 'version.txt')
SETUP_PY_PATH = 'setup.py'
README_PATH = 'README.md'
UPDATE_VERSION_SCRIPT = os.path.join('devscripts', 'update_version.py')
DOCS_DIR = 'docs'
BUILDDOCS_BAT = os.path.join(DOCS_DIR, 'builddocs.bat')


def read_current_version():
    with open(META_VERSION_PATH, 'r') as f:
        return f.read().strip()

def write_new_version(new_version):
    with open(META_VERSION_PATH, 'w') as f:
        f.write(new_version + '\n')

def update_version_in_file(current_version, new_version, file_path):
    subprocess.check_call([
        'python', UPDATE_VERSION_SCRIPT, current_version, new_version, file_path
    ])

def remove_dir(path):
    if os.path.exists(path):
        shutil.rmtree(path)

def build_package():
    subprocess.check_call(['python', 'setup.py', 'sdist', 'bdist_wheel'])

def build_docs():
    prev_cwd = os.getcwd()
    try:
        os.chdir(DOCS_DIR)
        subprocess.check_call(['cmd', '/c', 'builddocs.bat'])
    finally:
        os.chdir(prev_cwd)

def main():
    current_version = read_current_version()
    new_version = input(f'Enter the new version number (current is {current_version}): ').strip()
    write_new_version(new_version)
    update_version_in_file(current_version, new_version, SETUP_PY_PATH)
    update_version_in_file(current_version, new_version, README_PATH)
    print(f'Version updated from {current_version} to {new_version} in setup.py and README.md')
    remove_dir('build')
    remove_dir('dist')
    build_package()
    build_docs()
    print('Documentation built successfully.')

if __name__ == '__main__':
    main()
