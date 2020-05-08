# /bin/python

import rospkg
import subprocess
import os
import time
import shutil
import re

# src/smacc_ci/generate_debs.py -smacc_src_folder src -smacc_viewer_src_folder src


def build_deb_package(workspace_source_folder, package_name, packagepath, ubuntu_version, ros_distro, already_visited):
    print("-----------------------")
    print("Working folder: " + str(os.getcwd()))
    print("Building debian package: " + str(package_name))
    cmd = "bloom-generate rosdebian --os-name ubuntu --os-version " + \
        str(ubuntu_version) + " --ros-distro " + \
        str(ros_distro) + " " + str(packagepath)
    print(cmd)
    bloomprocess = subprocess.Popen(cmd, shell=True)
    bloomprocess.wait()

    develpackagefolder = None
    for root, dirs, files in os.walk(workspace_source_folder, topdown=False):
        if "package.xml" in files and package_name == root.split("/")[-1]:
            print(root)
            develpackagefolder = root
            break

    localpackagepath = develpackagefolder

    print("current directory:")
    print(os.getcwd())
    print("debian files:")
    print(os.listdir("."))

    # copy the debian folder generated by bloom inside the package
    shutil.move("debian", os.path.join(localpackagepath, "debian"))

    os.chdir(localpackagepath)
    fakerootprocess = subprocess.Popen(
        "fakeroot debian/rules binary", shell=True)
    fakerootprocess.wait()

    os.chdir(localpackagepath)
    shutil.rmtree("debian")
    shutil.rmtree("obj-x86_64-linux-gnu")

    os.chdir(workspace_source_folder)

    # improve this regex

    firstregexstr = '.*ros-' + ros_distro + '-.*\.deb'
    regexstr = '.*ros-' + ros_distro + '-' + \
        package_name.replace("_", "-")+".*\.deb"
    print("Finding deb package: " + str(regexstr))

    thisfolderfiles = []
    for root, dirs, files in os.walk(workspace_source_folder, topdown=False):
        thisfolderfiles = thisfolderfiles + \
            [os.path.join(root, f) for f in files]

    debianfiles = [f for f in thisfolderfiles if re.search(firstregexstr, f)]
    print ("DETECTED DEBIAN FILES:")
    print(debianfiles)
    print ("VISITED DEBIAN FILES:")
    print(already_visited)

    debianfilename = [f for f in debianfiles if re.search(
        regexstr, f) and not f in already_visited][0]

    print("Debian file found: ")
    print(debianfilename)

    installdebiantask = subprocess.Popen(
        "sudo dpkg -i " + debianfilename, shell=True)
    installdebiantask.wait()

    return debianfilename


def iterate_debian_generation(workspace_source_folder, package_names, identified_install_packages, osversion, rosversion):
    os.chdir(workspace_source_folder)
    debianfiles = []
    for pname in package_names:
        debianfiles.append(build_deb_package(workspace_source_folder,
                                             pname, identified_install_packages[pname], osversion, rosversion, debianfiles))
    return debianfiles


def get_identified_packages(workspace_folder):
    identified_install_packages = {}
    exclude_with_words = ["ridgeback", "mecanum", "catkin"]
    for pname in packagesl:
        packpath = rospack.get_path(pname)
        print(pname)
        if workspace_folder in packpath:
            if any([True for excludedword in exclude_with_words if excludedword in pname]):
                continue

            identified_install_packages[pname] = packpath
    return identified_install_packages


def push_debian_files(repo_owner, reponame,  osname, osversion, debianfiles):
    for debf in debianfiles:
        print("pushing debfile")
        push_debian_task = subprocess.Popen(
            "package_cloud push " + repo_owner+"/"+reponame+"/"+osname+"/" + osversion+" " + debf, shell=True)
        push_debian_task.wait()


def remove_debian_files(repo_owner, reponame,  osname, osversion, debianfiles):
    for debf in debianfiles:
        shortdebfile = debf.split("/")[-1]
        print("yanking debfile")
        push_debian_task = subprocess.Popen(
            "package_cloud yank " + repo_owner+"/"+reponame+"/"+osname+"/" + osversion+" " + shortdebfile, shell=True)
        push_debian_task.wait()

# ------------------------ SMACC PACKAGES -----------------------


def create_and_push_smacc_debians(osname, osversion, rosversion):
    workspace_source_folder = os.path.join(
        workspace_folder, relative_smacc_folder)
    identified_install_packages = get_identified_packages(workspace_folder)

    smacc_manual_order_packages = [  # 'forward_global_planner',
        'smacc_msgs',
        'smacc',
        'all_events_go',
        'conditional',
        'event_countdown',
        'keyboard_client',
        'move_base_z_client_plugin',
        'moveit_z_client',
        'multirole_sensor_client',
        'ros_publisher_client',
        'ros_timer_client',
        'sm_atomic',
        'sm_dance_bot',
        'sm_dance_bot_strikes_back',
        'sm_moveit',
    ]

    smacc_debian_files = iterate_debian_generation(
        workspace_source_folder, smacc_manual_order_packages, identified_install_packages, osversion, rosversion)

    create_repo_task = subprocess.Popen(
        "package_cloud repository create smacc", shell=True)
    create_repo_task.wait()

    # ----- PUSHING TO SMACC --------------
    remove_debian_files(repo_owner, "smacc",  osname,
                        osversion, smacc_debian_files)
    push_debian_files(repo_owner, "smacc",  osname,
                      osversion, smacc_debian_files)

    return smacc_debian_files


def create_and_push_smacc_viewer_debians(osname, osversion, rosversion):
    workspace_source_folder = os.path.join(
        workspace_folder, relative_smacc_viewer_folder)
    identified_install_packages = get_identified_packages(workspace_folder)
    smacc_viewer_manual_order_packages = ["smacc_viewer"]
    smacc_viewer_debian_files = iterate_debian_generation(
        workspace_source_folder, smacc_viewer_manual_order_packages, identified_install_packages, osversion, rosversion)

    create_repo_task = subprocess.Popen(
        "package_cloud repository create smacc_viewer", shell=True)
    create_repo_task.wait()

    # ----- PUSHING TO SMACC VIEWER--------------
    remove_debian_files(repo_owner, "smacc_viewer",  osname,
                        osversion, smacc_viewer_debian_files)
    push_debian_files(repo_owner, "smacc_viewer", osname,
                      osversion, smacc_viewer_debian_files)

    return smacc_viewer_debian_files


if __name__ == "__main__":
    # === requirements for the build machine ==
    # sudo apt-get install ruby-dev rake
    # sudo gem update --system
    # sudo gem install package_cloud
    # == OR ==
    # curl -s https://packagecloud.io/install/repositories/fdio/tutorial/script.deb.sh | sudo bash

    import argparse
    import argcomplete

    # ==== CONFIGURATION PARAMETERS =========
    repo_owner = "pibgeus"

    # =========================================

    rospack = rospkg.RosPack()
    packages = rospack.list()
    packagesl = list(packages)

    parser = argparse.ArgumentParser()
    parser.add_argument('-smacc_src_folder',
                        help="smacc workspace folder", default="src/SMACC")
    parser.add_argument('-smacc_viewer_src_folder',
                        help="relative smacc src folder", default="src/SMACC_Viewer")
    parser.add_argument('-repo_owner', help="Repo owner", default="pibgeus")
    parser.add_argument('-token', help="Repo token", default="")
    parser.add_argument(
        '-ros_version', help="The version of ros, ie: kinetic", default="kinetic", type=str)
    parser.add_argument(
        '-ubuntu_version', help="The version of ros, ie: xenial", default="xenial", type=str)
    parser.add_argument('-help', help="Help command")

    argcomplete.autocomplete(parser)

    args = parser.parse_args()
    # if hasattr(args,'help'):
    #     parser.print_help()

    osname = "ubuntu"
    osversion = args.ubuntu_version
    ros_version = args.ros_version

    print("args:" + str(args))
    print("rosversion: " + ros_version)
    print("ubuntu: " + osversion)

    relative_smacc_folder = args.smacc_src_folder  # "src/SMACC"
    relative_smacc_viewer_folder = args.smacc_viewer_src_folder  # "src/SMACC_Viewer"
    workspace_folder = os.path.abspath(os.path.join(os.getcwd(), "."))

    repo_owner = args.repo_owner

    print("CREATING TOKEN FILE FOR PACKAGE CLOUD:")
    homefolder = os.getenv("HOME")
    packagecloud_token_filepath = os.path.join(homefolder, ".packagecloud")

    outfile = open(packagecloud_token_filepath, "w")
    outfile.write('{"token":"%s"}' % args.token)
    outfile.close()

    smacc_debians = create_and_push_smacc_debians(
        osname, osversion, ros_version)
    smacc_viewer_debians = create_and_push_smacc_viewer_debians(
        osname, osversion, ros_version)

    print("SMACC DEBIANS: " + str(smacc_debians))
    print("SMACC VIEWER DEBIANS: " + str(smacc_viewer_debians))

    print("PUSHING SMACC_MSGS TO smacc_viewer repo")
    extra_smacc_viewer = [df for df in smacc_debians if "smacc-msgs" in df]
    print(extra_smacc_viewer)

    remove_debian_files(repo_owner, "smacc_viewer",  osname,
                        osversion, extra_smacc_viewer)
    push_debian_files(repo_owner, "smacc_viewer",  osname,
                      osversion, extra_smacc_viewer)
