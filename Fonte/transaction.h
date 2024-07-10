#define FTRANSACTION 1 //Flag para não reinclusão

void copy_data(rc_insert *data, rc_insert *copy, int op);

int add_op(Pilha *stack_log, int op, rc_insert* data);

void debug_stack_log(Pilha *stack_log);

void rollback(Pilha *stack_log);

void commit(Pilha *stack_log);

void drop_index(rc_insert *aux);

void rb_inicializa_indice(char* nomeTabela, char* nomeColuna);

void rb_incrementaQtdIndice(char* nomeTabela);

void rb_adicionaBT(char* nomeTabela, char* nomeColuna);