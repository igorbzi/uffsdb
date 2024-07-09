#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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
        if(data->columnName){
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
}

void add_op(Pilha *stack_log, int op, rc_insert* data){

    log_op *new = malloc(sizeof(log_op));

    rc_insert copy;
    copy_data(data, &copy, op);

    new->op = op;
    new->data = copy;

    push(stack_log, new);

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

void rollback(Pilha* stack_log){

    while(stack_log->tam > 0){

        log_op *log = pop(stack_log);
        rc_insert *aux = &log->data;

        switch(log->op){
            
            case OP_CREATE_TABLE:
                //drop table
                printf("Create Table: %s\n", aux->objName);
                excluirTabela(aux->objName);
            break;
            
            case OP_DROP_TABLE:
                //voltar a tabela
                printf("Drop Table: %s\n", aux->objName);
            break;

            case OP_INSERT:
                //remover inserção?
                printf("Insert Table: %s\n", aux->objName);
            break;

            case OP_CREATE_INDEX:
                //drop indice
                printf("Create Index: %s\n", aux->objName);
            break;

            default: break;

        }
    }
}