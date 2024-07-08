#define FTRANSACTION 1 //Flag para não reinclusão

void copy_data(rc_insert *data, rc_insert *copy, int op);

void add_op(Pilha *stack_log, int op, rc_insert* data);

void debug_stack_log(Pilha *stack_log);

void rollback(Pilha *stack_log);

