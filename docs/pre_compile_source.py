import sys
import os

# Append the relative directory to sys.path
sys.path.append(os.path.join("..", 'dbcore'))
from uainepydat import fileio
from uainepydat import datatransform

relative_directory = "../dbcore"  # Replace with your relative directory path

python_files = fileio.list_files_of_extension(os.path.abspath(relative_directory), "py")
modules = [os.path.splitext(os.path.basename(filepath))[0] for filepath in python_files]
# Remove __init__ from the module list
modules = [m for m in modules if m != "__init__"]
# Sort these modules out in alphabetical order
modules = sorted(modules)

#open pre-compile file and edit lines
pre_compile_path = "source/index.rst_pre"
pre_str = fileio.read_file_to_string(pre_compile_path)

purpose_path = "../meta/purpose.txt"
pur = fileio.read_file_to_string(purpose_path)
pur = datatransform.break_into_lines(pur)
post_str = datatransform.replace_between_tags(pre_str, "purpose", pur, deleteTags=True)

#add in changelog
changelog_path = "../meta/changelog.txt"
chlog = fileio.read_file_to_string(changelog_path)
chlog = datatransform.break_into_lines(chlog)
post_str = datatransform.replace_between_tags(post_str, "changelog", chlog, deleteTags=True)

#add description replacement
description_path = "../meta/description.txt"
if os.path.exists(description_path):
    description_content = fileio.read_file_to_string(description_path)
    description_content = datatransform.break_into_lines(description_content)
    post_str = datatransform.replace_between_tags(post_str, "description", description_content, deleteTags=True)
else:
    print(f"Warning: {description_path} does not exist. Skipping description replacement.")


post_compile_path = "source/index.rst"
#overwrite the index.rst now
with open(post_compile_path, "w") as text_file:
    text_file.write(post_str)
print("Updated rst file")
