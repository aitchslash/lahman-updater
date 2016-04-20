"""Unifies naming conventions for data files.

The first versions of this had a different data file naming system.
Written to move and rename files on via git commands.
If not using git, remove "git " from comand string.
"""

import os
from time import sleep


def rename_file(file_name):
    """Rename a file."""
    bit, ext = file_name.split('.')
    bits = bit.split('_')
    new_name = bits[2] + "_" + bits[1].lower() + '.' + ext
    return new_name.strip()


def main():
    """Main."""
    os.chdir(os.path.join('..', 'data', 'data2015'))
    og_data_dir = os.path.join('', 'fielding')
    print og_data_dir
    og_file_names = os.listdir(og_data_dir)
    git_names = {'fielding/' + og: rename_file(og) for og in og_file_names}
    for key in git_names.keys():
        command = "git mv " + key + " " + git_names[key]
        print command
        os.system(command)
        sleep(.5)
    os.system('rmdir fielding')
    print "Files moved and renamed. Containing directory removed."
    print "Your cwd has likely changed."

    return git_names
