#!/bin/bash -e

# Constants
BOLD_PINK='\e[35;1m'
RESET='\e[0m'
GREEN='\e[32;1m'
RED='\e[31;1m'
BLUE='\e[34;1m'

user_id=$(id -u)
user_name=$(whoami)
group_id=$(id -g)
group_name=$(id -gn)

# Define source and destination file paths
source_file=".devcontainer/devcontainer_base.json"
destination_file=".devcontainer/devcontainer.json"
destination_file_tmp=".devcontainer/devcontainer.json.tmp"

# Check if the source file exists
if [[ ! -f $source_file ]]; then
    printf "\n${RED}Source file ${source_file} does not exist. Exiting.${RESET}\n\n" >&2
    exit 1
fi

# Create the destination file with the updated content
sed -e "s/\"USER_UID\": \"1000\"/\"USER_UID\": \"$user_id\"/g" \
    -e "s/\"USER_GID\": \"1000\"/\"USER_GID\": \"$group_id\"/g" \
    -e "s/\"remoteUser\": \"ubuntu\"/\"remoteUser\": \"$user_name\"/g" \
    -e "s/\"USER_NAME\": \"ubuntu\"/\"USER_NAME\": \"$user_name\"/g" \
    -e "s/\"USER_GNAME\": \"ubuntu\"/\"USER_GNAME\": \"$group_name\"/g" \
    -e "s/USER=ubuntu/USER=$user_name/g" \
    "$source_file" > "$destination_file_tmp"

if [[ $? -eq 0 ]]; then
    if [[ ! -f "$destination_file" ]] || ! cmp -s "$destination_file" "$destination_file_tmp"; then
        cp $destination_file_tmp $destination_file
    fi
    rm $destination_file_tmp
    printf "\n${GREEN}Success!${RESET}\n\n"
    echo "  USER_UID: $user_id"
    echo "  USER_UNAME: $user_name"
    echo "  USER_GID: $group_id"
    echo "  USER_GNAME: $user_name"
else
    printf "\n${RED}Failed to create the file ${destination_file}.${RESET}\n\n" >&2
    exit 1
fi
