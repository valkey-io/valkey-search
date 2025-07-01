{
  global:
    # Only expose the ValkeyModule APIs
    ValkeyModule_OnLoad;
    ValkeyModule_OnUnload;
  local: *;  # hide everything else
};