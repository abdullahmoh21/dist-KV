void free_command(struct RedisCommand *command) {
    if (command->args == NULL) return;

    for (size_t i = 0; i < command->arg_count; i++) {
        // 1. Free the data pointer INSIDE the struct
        if (command->args[i].data != NULL) {
            free(command->args[i].data);
        }
    }
    // 2. Free the array itself
    free(command->args);
    
    // 3. Set to NULL to prevent accidental use (dangling pointer)
    command->args = NULL;
}