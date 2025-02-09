#!/bin/bash

user_id=$(id -u)
user_name=$(whoami)
group_id=$(id -g)
group_name=$(id -gn)

# Define source and destination file paths
source_file=".devcontainer/devcontainer_base.json"
destination_file=".devcontainer/devcontainer.json"

# Check if the source file exists
if [[ ! -f $source_file ]]; then
    echo "Source file $source_file does not exist. Exiting."
    exit 1
fi

# Create the destination file with the updated content
sed -e "s/\"USER_UID\": \"1000\"/\"USER_UID\": \"$user_id\"/g" \
    -e "s/\"USER_GID\": \"1000\"/\"USER_GID\": \"$group_id\"/g" \
    -e "s/\"remoteUser\": \"ubuntu\"/\"remoteUser\": \"$user_name\"/g" \
    -e "s/\"USER_NAME\": \"ubuntu\"/\"USER_NAME\": \"$user_name\"/g" \
    -e "s/\"USER_GNAME\": \"ubuntu\"/\"USER_GNAME\": \"$group_name\"/g" \
    -e "s/USER=ubuntu/USER=$user_name/g" \
    "$source_file" > "$destination_file"

# Verify the operation and provide feedback
if [[ $? -eq 0 ]]; then
    echo "File successfully created at $destination_file with updated user information:"
    echo "  USER_UID: $user_id"
    echo "  USER_UNAME: $user_name"
    echo "  USER_GID: $group_id"
    echo "  USER_GNAME: $user_name"
else
    echo "Failed to create the file $destination_file."
    exit 2
fi
