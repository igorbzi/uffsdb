#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#ifndef FMACROS // garante que macros.h não seja reincluída
   #include "macros.h"
#endif
///
#ifndef FTYPES // garante que types.h não seja reincluída
  #include "types.h"
#endif
#ifndef FUTILITY
   #include "Utility.h"
#endif
#ifndef FPARSER
    #include "interface/parser.h"
#endif
#ifndef FSQLCOMMANDS
    #include "sqlcommands.h"
#endif
#ifndef FDICTIONARY
    #include "dictionary.h"
#endif
#include "transaction.h"

void copy_data(rc_insert *data, rc_insert *copy, int op){

    copy->objName = malloc(sizeof(data->objName));
    strcpy(copy->objName, data->objName);
    copy->N = data->N;

    //create index só usa objName e columnName[0], para identificar a coluna referente ao indice
    if(op == OP_CREATE_INDEX){
        copy->columnName = malloc(sizeof(char *));
        copy->columnName[0] = malloc(sizeof(data->columnName[0]));
        strcpy(copy->columnName[0], data->columnName[0]);
        //printf("copy->columnName[0]= %s\n", copy->columnName[0]);
        return;
    }

    //Alocando memória
    copy->columnName = malloc(sizeof(char *) * data->N);
    copy->values = malloc(sizeof(char *) * data->N);
    copy->type = malloc(sizeof(char) * data->N);
    copy->fkTable = malloc(sizeof(char*) * data->N);
    copy->fkColumn = malloc(sizeof(char *) * data->N);

    //atributos só usados em criação de tabelas
    if(op  == OP_CREATE_TABLE){
        copy->attribute = malloc(sizeof(int) * data->N);
    }

    //Copiando valores
    for(int i=0; i<data->N; i++){

        if(data->type){
            copy->type[i] = data->type[i];
            //printf("Copy->type[%d]= %c\n", i, copy->type[i]);
        }
        if(data->attribute && op == OP_CREATE_TABLE){
            copy->attribute[i] = data->attribute[i];
            //printf("Copy->attribute[%d]= %c\n", i, copy->attribute[i]);
        }
        if(data->columnName && op != OP_INSERT){
            copy->columnName[i] = malloc(sizeof(data->columnName[i]));
            strcpy(copy->columnName[i], data->columnName[i]);
            //printf("Copy->columnName[%d]= %s\n", i, copy->columnName[i]);
        }
        if(data->values){
            copy->values[i] = malloc(sizeof(data->values[i]));
            strcpy(copy->values[i], data->values[i]);
            //printf("Copy->values[%d]= %s\n", i, copy->values[i]);
        }
        if(data->fkTable){
            copy->fkTable[i] = malloc(sizeof(data->fkTable[i]));
            strcpy(copy->fkTable[i], data->fkTable[i]);
            //printf("Copy->fkTable[%d]= %s\n", i, copy->fkTable[i]);
        }
        if(data->fkColumn){
            copy->fkColumn[i] = malloc(sizeof(data->fkColumn[i]));
            strcpy(copy->fkColumn[i], data->fkColumn[i]);
            //printf("Copy->fkColumn[%d]= %s\n", i, copy->fkColumn[i]);
        }
    }

    if(op == OP_INSERT){
        copy->columnName = NULL;
    }
}

int add_op(Pilha *stack_log, int op, rc_insert* data){

    log_op *new = malloc(sizeof(log_op));

    rc_insert copy;
    copy_data(data, &copy, op);
    new->op = op;
    new->data = copy;

    push(stack_log, new);
    return SUCCESS;
}

void debug_stack_log(Pilha *stack_log){


    if(stack_log->topo == NULL) {
        printf("Stack is empty\n");
        return;
    }

    printf("DEBUGGING:\n\n");

    while(stack_log->tam > 0){

        log_op *log = pop(stack_log);
        rc_insert *aux = &log->data;

        switch(log->op){
            
            case OP_CREATE_TABLE:
                printf("Create Table: %s\n", aux->objName);
            break;
            case OP_DROP_TABLE:
                printf("Drop Table: %s\n", aux->objName);
            break;
            case OP_INSERT:
                printf("Insert Table: %s\n", aux->objName);
            break;
            case OP_CREATE_INDEX:
                printf("Create Index: %s\n", aux->objName);
            break;
            default: break;

        }
    }
}

void rb_drop_table(char *nomeTabela){
    // Caminhos para os arquivos de backup
    char backup_object[LEN_DB_NAME_IO * 2];
    char backup_schema[LEN_DB_NAME_IO * 2];
    char backup_data[LEN_DB_NAME_IO * 2];
    char backup_id[LEN_DB_NAME_IO * 2];
    char original_object[LEN_DB_NAME_IO * 2];
    char original_schema[LEN_DB_NAME_IO * 2];
    char original_data[LEN_DB_NAME_IO * 2];
    char original_id[LEN_DB_NAME_IO * 2];

    snprintf(backup_object, sizeof(backup_object), "%sfs_object_backup_%s.dat", connected.db_directory, nomeTabela);
    snprintf(backup_schema, sizeof(backup_schema), "%sfs_schema_backup_%s.dat", connected.db_directory, nomeTabela);
    snprintf(backup_data, sizeof(backup_data), "%s%s_backup.dat", connected.db_directory, nomeTabela);
    snprintf(backup_id, sizeof(backup_id), "%s%sid_backup.dat", connected.db_directory, nomeTabela);

    snprintf(original_object, sizeof(original_object), "%sfs_object.dat", connected.db_directory);
    snprintf(original_schema, sizeof(original_schema), "%sfs_schema.dat", connected.db_directory);
    snprintf(original_data, sizeof(original_data), "%s%s.dat", connected.db_directory, nomeTabela);
    snprintf(original_id, sizeof(original_id), "%s%sid.dat", connected.db_directory, nomeTabela);

    // Verifica se os arquivos de backup existem
    if (access(backup_object, F_OK) != -1) {
        char cmd[LEN_DB_NAME_IO * 4];
        snprintf(cmd, sizeof(cmd), "cp %s %s", backup_object, original_object);
        system(cmd);
        snprintf(cmd, sizeof(cmd), "cp %s %s", backup_schema, original_schema);
        system(cmd);

        // Verifica se o backup de dados existe antes de restaurar
        if (access(backup_data, F_OK) != -1){
            snprintf(cmd, sizeof(cmd), "cp %s %s", backup_data, original_data);
            system(cmd);
        }

        // Verifica se o backup do ID existe antes de restaurar
        if (access(backup_id, F_OK) != -1){
            snprintf(cmd, sizeof(cmd), "cp %s %s", backup_id, original_id);
            system(cmd);
        }
    }

    // Remove os arquivos de backup após restauração
    remove(backup_object);
    remove(backup_schema);
    remove(backup_data);
    remove(backup_id);
}
    
void rollback(Pilha* stack_log){

    while(stack_log->tam > 0){

        log_op *log = pop(stack_log);
        rc_insert *aux = &log->data;

        switch(log->op){
            
            case OP_CREATE_TABLE:
                //drop table
                excluirTabela(aux->objName);
            break;
            
            case OP_DROP_TABLE:
                //voltar a tabela
                rb_drop_table(aux->objName);
            break;

            case OP_INSERT:
                //remover inserção?
            break;

            case OP_CREATE_INDEX:
                drop_index(aux);
            break;

            default: break;

        }
    }
}

void commit(Pilha *stack_log){

    while(stack_log->tam > 0){

        log_op *log = pop(stack_log);
        rc_insert *aux = &log->data;

        if(log->op == OP_INSERT){
            if(aux->N > 0){
                insert(aux);
            }
        }
        else if(log->op == OP_DROP_TABLE){
            char backup_object[LEN_DB_NAME_IO * 2];
            char backup_schema[LEN_DB_NAME_IO * 2];
            char backup_data[LEN_DB_NAME_IO * 2];
            char backup_id[LEN_DB_NAME_IO * 2];
            snprintf(backup_object, sizeof(backup_object), "%sfs_object_backup_%s.dat", connected.db_directory, aux->objName);
            snprintf(backup_schema, sizeof(backup_schema), "%sfs_schema_backup_%s.dat", connected.db_directory, aux->objName);
            snprintf(backup_data, sizeof(backup_data), "%s%s_backup.dat", connected.db_directory, aux->objName);
            snprintf(backup_id, sizeof(backup_id), "%s%sid_backup.dat", connected.db_directory, aux->objName);

            remove(backup_object);
            remove(backup_schema);
            remove(backup_data);
            remove(backup_id);
        }
    }
}

void drop_index(rc_insert *aux) {
    char*  nomeTabela = aux->objName;
    char*  nomeColuna = aux->columnName[0];
    //desfaz as alterações de inicializa_indice()
    rb_inicializa_indice(nomeTabela, nomeColuna);
    //desfaz as alterações de incrementaQtdIndice()
    rb_incrementaQtdIndice(nomeTabela);
    //desfaz alterações de adicionaBT()
    rb_adicionaBT(nomeTabela, aux->columnName[0]);

}

void rb_inicializa_indice(char* nomeTabela, char* nomeColuna){
    char dir[TAMANHO_NOME_TABELA + TAMANHO_NOME_ARQUIVO + TAMANHO_NOME_CAMPO + TAMANHO_NOME_INDICE];
    strcpy(dir, connected.db_directory);
    strcat(dir, nomeTabela);
    strcat(dir, nomeColuna);
    strcat(dir, ".dat");
    remove(dir); // Remove o arquivo de índice criado
    return;
}

void rb_incrementaQtdIndice(char* nomeTabela){
    FILE *dicionario = NULL;
    char tupla[TAMANHO_NOME_TABELA];
    memset(tupla, '\0', TAMANHO_NOME_TABELA);
    int qt = 0, offset = 0;

    char directory[LEN_DB_NAME_IO];
    strcpy(directory, connected.db_directory);
    strcat(directory, "fs_object.dat");

    if ((dicionario = fopen(directory,"r+b")) == NULL) {
        printf("Erro ao abrir dicionário de dados.\n");
        return;
    }

    while (fgetc(dicionario) != EOF) {
        fseek(dicionario, -1, 1);

        fread(tupla, sizeof(char), TAMANHO_NOME_TABELA, dicionario);
        if (strcmp(tupla, nomeTabela) == 0) {
            fseek(dicionario, sizeof(int), SEEK_CUR); // Pula o código da tabela
            fseek(dicionario, sizeof(char) * TAMANHO_NOME_ARQUIVO, SEEK_CUR); // Pula o nome do arquivo da tabela
            fseek(dicionario, sizeof(int), SEEK_CUR); // Pula a quantidade de campos da tabela
            offset = ftell(dicionario); // Guarda a posição atual do ponteiro
            fread(&qt, sizeof(int), 1, dicionario); // Lê a quantidade de índices
            qt--; // Decrementa
            fseek(dicionario, offset, SEEK_SET); // Move o ponteiro de volta para a posição original
            fwrite(&qt, sizeof(int), 1, dicionario); // Escreve o valor decrementado de volta no arquivo
            fclose(dicionario); // Fecha o arquivo
            return;
        }
        fseek(dicionario, 32, 1);
    }
    printf("Erro ao atualizar dicionário de dados.\n");

}

void rb_adicionaBT(char* nomeTabela, char* nomeColuna){
    int cod;
    char directory[LEN_DB_NAME_IO];
    strcpy(directory, connected.db_directory); // Copia o diretório do banco de dados para 'directory'
    strcat(directory, "fs_schema.dat"); // Adiciona o nome do arquivo ao diretório

    char atrib[TAMANHO_NOME_CAMPO];
    memset(atrib, '\0', TAMANHO_NOME_CAMPO); // Inicializa 'atrib' com zeros

    FILE *schema = fopen(directory, "r+b"); // Abre o arquivo 'fs_schema.dat'
    if (schema == NULL) {
        printf("Erro ao abrir esquema da tabela.\n");
        return;
    }

    struct fs_objects objeto = leObjeto(nomeTabela); // Lê os dados do objeto da tabela

    // Percorre o arquivo até o final
    while (fgetc(schema) != EOF) {
        fseek(schema, -1, 1); // Move o ponteiro de volta uma posição

        if (fread(&cod, sizeof(int), 1, schema)) { // Lê o código da tabela
            if (cod == objeto.cod) { // Verifica se o código da tabela corresponde ao código do objeto
                fread(atrib, sizeof(char), TAMANHO_NOME_CAMPO, schema); // Lê o nome do campo
                if (strcmp(atrib, nomeColuna) == 0) { // Verifica se o nome do campo corresponde ao nome do atributo
                    fseek(schema, 5, SEEK_CUR); // Pula 1 byte do tipo e 4 bytes do tamanho
                    int chave = NPK; // Valor original da chave
                    fwrite(&chave, sizeof(int), 1, schema); // Escreve o valor original da chave
                    fclose(schema); // Fecha o arquivo
                    return;
                } else {
                    fseek(schema, 69, SEEK_CUR); // Pula para a próxima entrada (69 bytes)
                }
            } else {
                fseek(schema, 109, 1); // Pula para a próxima entrada (109 bytes)
            }
        }
    }

    printf("Erro ao abrir o esquema da tabela \"%s\".\n", nomeTabela);
}