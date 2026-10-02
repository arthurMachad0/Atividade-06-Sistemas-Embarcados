/*
 * Cinco tarefas produtoras (Temperatura, Umidade, Velocidade, Peso, Distancia)
 * escrevem o proprio nome 5 vezes em um buffer global de 10 posicoes (vetor de
 * strings, NAO thread safe). Duas tarefas leitoras (TaskLeitura1 e
 * TaskLeitura2) consomem as posicoes.
 *
 * Semaforos usados:
 *  - mutex_do_buffer             : exclusao mutua no acesso ao buffer e aos indices.
 *  - semaforo_posicoes_livres    : contador (inicia em 10). Produtora espera se o buffer encheu.
 *  - semaforo_posicoes_ocupadas  : contador (inicia em 0). Leitora espera ter dado disponivel.
 *
 * Como sao 25 escritas (5 x 5) para 10 posicoes, o buffer e circular: cada
 * leitura libera uma posicao para as produtoras.
 *
 * Regra de termino das leitoras: elas so se deletam quando o buffer esta vazio
 * E todas as produtoras ja terminaram. Se o buffer esvaziar apenas
 * momentaneamente, a leitora continua aguardando.
 */

#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/semphr.h"

#define QUANTIDADE_DE_PRODUTORAS      5
#define QUANTIDADE_DE_LEITORAS        2
#define ESCRITAS_POR_PRODUTORA        5
#define TAMANHO_DO_BUFFER             10
#define TAMANHO_MAXIMO_DO_DADO        16
#define TAMANHO_PILHA_TAREFAS         4096
#define PRIORIDADE_DAS_TAREFAS        5
#define ATRASO_ENTRE_ESCRITAS_MS      20
#define ATRASO_ENTRE_LEITURAS_MS      30
#define ESPERA_MAXIMA_DA_LEITORA_MS   100

/* Recurso compartilhado: variavel global comum, sem protecao propria. */
static char buffer_compartilhado[TAMANHO_DO_BUFFER][TAMANHO_MAXIMO_DO_DADO];
static int  indice_de_escrita   = 0;
static int  indice_de_leitura   = 0;
static int  produtoras_ativas   = QUANTIDADE_DE_PRODUTORAS;

static SemaphoreHandle_t mutex_do_buffer;
static SemaphoreHandle_t semaforo_posicoes_livres;
static SemaphoreHandle_t semaforo_posicoes_ocupadas;

static const char *nomes_das_produtoras[QUANTIDADE_DE_PRODUTORAS] = {
    "Temperatura", "Umidade", "Velocidade", "Peso", "Distancia"
};
static const char *nomes_das_leitoras[QUANTIDADE_DE_LEITORAS] = {
    "TaskLeitura1", "TaskLeitura2"
};

static void tarefa_produtora(void *parametros)
{
    (void)parametros;
    const char *meu_nome = pcTaskGetName(NULL);

    for (int escrita_atual = 0; escrita_atual < ESCRITAS_POR_PRODUTORA; escrita_atual++) {
        /* Espera haver posicao livre e, depois, acesso exclusivo ao buffer. */
        xSemaphoreTake(semaforo_posicoes_livres, portMAX_DELAY);
        xSemaphoreTake(mutex_do_buffer, portMAX_DELAY);

        strncpy(buffer_compartilhado[indice_de_escrita], meu_nome, TAMANHO_MAXIMO_DO_DADO - 1);
        buffer_compartilhado[indice_de_escrita][TAMANHO_MAXIMO_DO_DADO - 1] = '\0';
        indice_de_escrita = (indice_de_escrita + 1) % TAMANHO_DO_BUFFER;

        xSemaphoreGive(mutex_do_buffer);
        xSemaphoreGive(semaforo_posicoes_ocupadas);   /* avisa as leitoras */

        vTaskDelay(pdMS_TO_TICKS(ATRASO_ENTRE_ESCRITAS_MS));
    }

    printf("%s: Escrita Finalizada.\n", meu_nome);

    /* Informa que esta produtora terminou (depois da ultima escrita). */
    xSemaphoreTake(mutex_do_buffer, portMAX_DELAY);
    produtoras_ativas--;
    xSemaphoreGive(mutex_do_buffer);

    vTaskDelete(NULL);
}

static void tarefa_leitora(void *parametros)
{
    (void)parametros;
    const char *meu_nome = pcTaskGetName(NULL);
    char dado_lido[TAMANHO_MAXIMO_DO_DADO];

    while (1) {
        bool conseguiu_dado = (xSemaphoreTake(semaforo_posicoes_ocupadas,
                               pdMS_TO_TICKS(ESPERA_MAXIMA_DA_LEITORA_MS)) == pdTRUE);

        if (!conseguiu_dado) {
            /* Buffer vazio: so encerra se nenhuma produtora ainda for escrever. */
            xSemaphoreTake(mutex_do_buffer, portMAX_DELAY);
            int produtoras_restantes = produtoras_ativas;
            xSemaphoreGive(mutex_do_buffer);

            if (produtoras_restantes > 0) {
                continue;
            }

            /* Ultima tentativa: pode ter entrado dado logo antes da produtora sair. */
            conseguiu_dado = (xSemaphoreTake(semaforo_posicoes_ocupadas, 0) == pdTRUE);
            if (!conseguiu_dado) {
                break;
            }
        }

        xSemaphoreTake(mutex_do_buffer, portMAX_DELAY);
        strncpy(dado_lido, buffer_compartilhado[indice_de_leitura], TAMANHO_MAXIMO_DO_DADO - 1);
        dado_lido[TAMANHO_MAXIMO_DO_DADO - 1] = '\0';
        indice_de_leitura = (indice_de_leitura + 1) % TAMANHO_DO_BUFFER;
        xSemaphoreGive(mutex_do_buffer);

        xSemaphoreGive(semaforo_posicoes_livres);     /* libera a posicao para as produtoras */

        printf("%s: %s\n", meu_nome, dado_lido);
        vTaskDelay(pdMS_TO_TICKS(ATRASO_ENTRE_LEITURAS_MS));
    }

    printf("%s: Leitura finalizada!\n", meu_nome);
    vTaskDelete(NULL);
}

void app_main(void)
{
    mutex_do_buffer            = xSemaphoreCreateMutex();
    semaforo_posicoes_livres   = xSemaphoreCreateCounting(TAMANHO_DO_BUFFER, TAMANHO_DO_BUFFER);
    semaforo_posicoes_ocupadas = xSemaphoreCreateCounting(TAMANHO_DO_BUFFER, 0);

    for (int i = 0; i < QUANTIDADE_DE_PRODUTORAS; i++) {
        xTaskCreate(tarefa_produtora, nomes_das_produtoras[i], TAMANHO_PILHA_TAREFAS,
                    NULL, PRIORIDADE_DAS_TAREFAS, NULL);
    }
    for (int i = 0; i < QUANTIDADE_DE_LEITORAS; i++) {
        xTaskCreate(tarefa_leitora, nomes_das_leitoras[i], TAMANHO_PILHA_TAREFAS,
                    NULL, PRIORIDADE_DAS_TAREFAS, NULL);
    }
}
