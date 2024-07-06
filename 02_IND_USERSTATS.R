library(dplyr)
library(gt)
library(sparklyr)
library(viridis)
library(sparkavro)
library(tidyverse)
library(lubridate)
library(readr)


spark_disconnect_all()
config <- list()
config$`sparklyr.cores.local` <- 4
config$`sparklyr.shell.driver-memory` <- "8G"
config$spark.memory.fraction <- 0.9
config$spark.sql.parquet.datetimeRebaseModeInWrite <- "LEGACY"
config$spark.sql.legacy.timeParserPolicy<- "LEGACY"
config$spark.driver.maxResultSize <- "2048m"
sc <- spark_connect(master = "local", version = "3.5.1",config=config)

setwd("D:/Dataset/Unir/data_tfm/TFM_PROD")

USER_ID_EX <- 'af8544ab-d7dd-4f9b-bfb1-0150879c8f02'

proc_pivot_contadores <- function(dfPivot,nroSemanas,suffix,datorg){
  n=ncol(dfPivot)
  k=nroSemanas
  col=colnames(dfPivot)[(n-k+1):n]
  lcol=colnames(dfPivot)[(n-k+1):n]
  col=paste0("`",col,"`")
  col=paste0("coalesce(",col,",0)")
  
  if(nroSemanas==0){
    per = tail(col, 1)
    cont <- dfPivot %>%
      mutate(per_cont = sql(per)) %>% 
      select(1, contains("_cont")) %>%
      rename_all(funs(c("key_id",
                        paste0(suffix, "_", "peranalisis"))))
    return(cont)
  }
  
  sum = paste("(", paste0(col, collapse = " + "), ")")
  prom = paste("(", paste0(col, collapse = " + "), ")", "/", k)
  diff = paste0(tail(col, 1), "-", col[1])
  var = paste0("(", tail(col, 1), "/", col[1], ")", "- 1")
  max = paste0("greatest(", paste0(col, collapse = ","), ")")
  min = paste0("least(", paste0(col, collapse = ","), ")")
  mes = col[1]
  
  var_analisis <- colnames(datorg)[3]
  df_med <- datorg %>% 
    filter(PERIODO_WEEK %in% lcol) %>% 
    dplyr::group_by(key_id) %>% 
    summarise(median=percentile(!!as.name(var_analisis),0.5)) %>% 
    rename_all(funs(
      c(
        "key_id",
        paste0(suffix, "_", "median", k, "s")
      )
    ))
  
  cont <- dfPivot %>%
    mutate(
      sum_cont = sql(sum),
      prom_cont = round(sql(prom), 3),
      diff_cont = sql(diff),
      var_cont = round(sql(var), 3),
      max_cont = sql(max),
      min_cont = sql(min),
      mes_cont = sql(mes)
    ) %>%
    select(1, contains("_cont")) %>%
    rename_all(funs(
      c(
        "key_id",
        paste0(suffix, "_", "sum", k, "s"),
        paste0(suffix, "_", "prom", k, "s"),
        paste0(suffix, "_", "diff", k, "s"),
        paste0(suffix, "_", "var", k, "s"),
        paste0(suffix, "_", "max", k, "s"),
        paste0(suffix, "_", "min", k, "s"),
        paste0(suffix, "_", "sem", k)
      )
    )) %>% left_join(.,df_med,by="key_id")
  return(cont)
}


args = commandArgs(trailingOnly=TRUE)
if (length(args)!=0) {
  PERIODO_ANALISIS <- as.integer(args[1])
}

# PERIODO_ANALISIS <- 6

# Carga de eventos
userstats <- spark_read_parquet(sc,"preprocess_data/sst_userstats_summarize") %>% 
  filter(PERIODO_WEEK<=PERIODO_ANALISIS) %>% 
  mutate(key_id=paste0(SITE_UID,"_",USER_ID),
         PERIODO_WEEK=as.integer(PERIODO_WEEK)) %>% 
  select(-SITE_UID,-USER_ID)

#################### tt_login ####################################
dat_tt_login <- userstats %>%
  select(PERIODO_WEEK,key_id,tt_login)

dat_pivot_tt_login <- dat_tt_login %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_login = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_login <-
  proc_pivot_contadores(dat_pivot_tt_login,0,'tt_login',dat_tt_login) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_login,2,'tt_login',dat_tt_login),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_login,3,'tt_login',dat_tt_login),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_login,4,'tt_login',dat_tt_login),by='key_id') %>%
  sdf_persist()


userstats %>% 
  filter(PERIODO_WEEK==PERIODO_ANALISIS) %>% 
  select(PERIODO_WEEK,key_id) %>% distinct() %>%
  full_join(.,cont_tt_login,by='key_id') %>% 
  mutate(PERIODO_WEEK=PERIODO_ANALISIS) %>% 
  sdf_coalesce(1) %>% spark_write_csv("preprocess_data/indicadores_userstats_sakai_newdata",mode="append",partition_by = "PERIODO_WEEK")









