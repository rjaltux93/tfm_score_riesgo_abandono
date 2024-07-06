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
resources <- spark_read_parquet(sc,"preprocess_data/sst_resources_summarize") %>% 
  filter(PERIODO_WEEK<=PERIODO_ANALISIS) %>% 
  mutate(key_id=paste0(SITE_UID,"_",USER_ID)) %>% 
  select(-SITE_UID,-USER_ID)

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


#################### tres_up_foros ####################################
dat_tres_up_foros <- resources %>%
  select(PERIODO_WEEK,key_id,tres_up_foros)  

dat_pivot_tres_up_foros <- dat_tres_up_foros %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tres_up_foros = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tres_up_foros <-
  proc_pivot_contadores(dat_pivot_tres_up_foros,0,'tres_up_foros',dat_tres_up_foros) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tres_up_foros,2,'tres_up_foros',dat_tres_up_foros),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tres_up_foros,3,'tres_up_foros',dat_tres_up_foros),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tres_up_foros,4,'tres_up_foros',dat_tres_up_foros),by='key_id') %>%
  sdf_persist()


#################### tres_down_foros ####################################
dat_tres_down_foros <- resources %>%
  select(PERIODO_WEEK,key_id,tres_down_foros)

dat_pivot_tres_down_foros <- dat_tres_down_foros %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tres_down_foros = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tres_down_foros <-
  proc_pivot_contadores(dat_pivot_tres_down_foros,0,'tres_down_foros',dat_tres_down_foros) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tres_down_foros,2,'tres_down_foros',dat_tres_down_foros),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tres_down_foros,3,'tres_down_foros',dat_tres_down_foros),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tres_down_foros,4,'tres_down_foros',dat_tres_down_foros),by='key_id') %>%
  sdf_persist()


#################### tres_up_doc ####################################
dat_tres_up_doc <- resources %>%
  select(PERIODO_WEEK,key_id,tres_up_doc)

dat_pivot_tres_up_doc <- dat_tres_up_doc %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tres_up_doc = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tres_up_doc <-
  proc_pivot_contadores(dat_pivot_tres_up_doc,0,'tres_up_doc',dat_tres_up_doc) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tres_up_doc,2,'tres_up_doc',dat_tres_up_doc),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tres_up_doc,3,'tres_up_doc',dat_tres_up_doc),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tres_up_doc,4,'tres_up_doc',dat_tres_up_doc),by='key_id') %>%
  sdf_persist()


#################### tres_down_doc ####################################
dat_tres_down_doc <- resources %>%
  select(PERIODO_WEEK,key_id,tres_down_doc)

dat_pivot_tres_down_doc <- dat_tres_down_doc %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tres_down_doc = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tres_down_doc <-
  proc_pivot_contadores(dat_pivot_tres_down_doc,0,'tres_down_doc',dat_tres_down_doc) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tres_down_doc,2,'tres_down_doc',dat_tres_down_doc),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tres_down_doc,3,'tres_down_doc',dat_tres_down_doc),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tres_down_doc,4,'tres_down_doc',dat_tres_down_doc),by='key_id') %>%
  sdf_persist()


#################### tres_up_msg ####################################
dat_tres_up_msg <- resources %>%
  select(PERIODO_WEEK,key_id,tres_up_msg)

dat_pivot_tres_up_msg <- dat_tres_up_msg %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tres_up_msg = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tres_up_msg <-
  proc_pivot_contadores(dat_pivot_tres_up_msg,0,'tres_up_msg',dat_tres_up_msg) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tres_up_msg,2,'tres_up_msg',dat_tres_up_msg),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tres_up_msg,3,'tres_up_msg',dat_tres_up_msg),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tres_up_msg,4,'tres_up_msg',dat_tres_up_msg),by='key_id') %>%
  sdf_persist()


#################### tres_down_msg ####################################
dat_tres_down_msg <- resources %>%
  select(PERIODO_WEEK,key_id,tres_down_msg)

dat_pivot_tres_down_msg <- dat_tres_down_msg %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tres_down_msg = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tres_down_msg <-
  proc_pivot_contadores(dat_pivot_tres_down_msg,0,'tres_down_msg',dat_tres_down_msg) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tres_down_msg,2,'tres_down_msg',dat_tres_down_msg),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tres_down_msg,3,'tres_down_msg',dat_tres_down_msg),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tres_down_msg,4,'tres_down_msg',dat_tres_down_msg),by='key_id') %>%
  sdf_persist()


#################### tres_up_tareas ####################################
dat_tres_up_tareas <- resources %>%
  select(PERIODO_WEEK,key_id,tres_up_tareas)

dat_pivot_tres_up_tareas <- dat_tres_up_tareas %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tres_up_tareas = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tres_up_tareas <-
  proc_pivot_contadores(dat_pivot_tres_up_tareas,0,'tres_up_tareas',dat_tres_up_tareas) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tres_up_tareas,2,'tres_up_tareas',dat_tres_up_tareas),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tres_up_tareas,3,'tres_up_tareas',dat_tres_up_tareas),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tres_up_tareas,4,'tres_up_tareas',dat_tres_up_tareas),by='key_id') %>%
  sdf_persist()


#################### tres_down_tareas ####################################
dat_tres_down_tareas <- resources %>%
  select(PERIODO_WEEK,key_id,tres_down_tareas)

dat_pivot_tres_down_tareas <- dat_tres_down_tareas %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tres_down_tareas = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tres_down_tareas <-
  proc_pivot_contadores(dat_pivot_tres_down_tareas,0,'tres_down_tareas',dat_tres_down_tareas) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tres_down_tareas,2,'tres_down_tareas',dat_tres_down_tareas),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tres_down_tareas,3,'tres_down_tareas',dat_tres_down_tareas),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tres_down_tareas,4,'tres_down_tareas',dat_tres_down_tareas),by='key_id') %>%
  sdf_persist()


#################### tres_up_prog ####################################
dat_tres_up_prog <- resources %>%
  select(PERIODO_WEEK,key_id,tres_up_prog)

dat_pivot_tres_up_prog <- dat_tres_up_prog %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tres_up_prog = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tres_up_prog <-
  proc_pivot_contadores(dat_pivot_tres_up_prog,0,'tres_up_prog',dat_tres_up_prog) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tres_up_prog,2,'tres_up_prog',dat_tres_up_prog),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tres_up_prog,3,'tres_up_prog',dat_tres_up_prog),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tres_up_prog,4,'tres_up_prog',dat_tres_up_prog),by='key_id') %>%
  sdf_persist()


#################### tres_down_prog ####################################
dat_tres_down_prog <- resources %>%
  select(PERIODO_WEEK,key_id,tres_down_prog)

dat_pivot_tres_down_prog <- dat_tres_down_prog %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tres_down_prog = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tres_down_prog <-
  proc_pivot_contadores(dat_pivot_tres_down_prog,0,'tres_down_prog',dat_tres_down_prog) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tres_down_prog,2,'tres_down_prog',dat_tres_down_prog),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tres_down_prog,3,'tres_down_prog',dat_tres_down_prog),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tres_down_prog,4,'tres_down_prog',dat_tres_down_prog),by='key_id') %>%
  sdf_persist()


#################### tres_up_otros ####################################
dat_tres_up_otros <- resources %>%
  select(PERIODO_WEEK,key_id,tres_up_otros)

dat_pivot_tres_up_otros <- dat_tres_up_otros %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tres_up_otros = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tres_up_otros <-
  proc_pivot_contadores(dat_pivot_tres_up_otros,0,'tres_up_otros',dat_tres_up_otros) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tres_up_otros,2,'tres_up_otros',dat_tres_up_otros),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tres_up_otros,3,'tres_up_otros',dat_tres_up_otros),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tres_up_otros,4,'tres_up_otros',dat_tres_up_otros),by='key_id') %>%
  sdf_persist()


#################### tres_down_otros ####################################
dat_tres_down_otros <- resources %>%
  select(PERIODO_WEEK,key_id,tres_down_otros)

dat_pivot_tres_down_otros <- dat_tres_down_otros %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tres_down_otros = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tres_down_otros <-
  proc_pivot_contadores(dat_pivot_tres_down_otros,0,'tres_down_otros',dat_tres_down_otros) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tres_down_otros,2,'tres_down_otros',dat_tres_down_otros),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tres_down_otros,3,'tres_down_otros',dat_tres_down_otros),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tres_down_otros,4,'tres_down_otros',dat_tres_down_otros),by='key_id') %>%
  sdf_persist()

dfinal1 <- 
  resources %>% 
  filter(PERIODO_WEEK==PERIODO_ANALISIS) %>% 
  select(PERIODO_WEEK,key_id) %>% distinct() %>% 
  full_join(.,cont_tres_up_foros,by='key_id') %>% 
  full_join(.,cont_tres_down_foros,by='key_id') %>% 
  full_join(.,cont_tres_up_doc,by='key_id') %>% 
  full_join(.,cont_tres_down_doc,by='key_id') %>% 
  full_join(.,cont_tres_up_msg,by='key_id') %>% 
  full_join(.,cont_tres_down_msg,by='key_id') %>% 
  full_join(.,cont_tres_up_tareas,by='key_id') %>% 
  full_join(.,cont_tres_down_tareas,by='key_id') %>% 
  full_join(.,cont_tres_up_prog,by='key_id') %>% 
  full_join(.,cont_tres_down_prog,by='key_id') %>% 
  full_join(.,cont_tres_up_otros,by='key_id') %>% 
  full_join(.,cont_tres_down_otros,by='key_id') 


dfinal1 %>%
  mutate(PERIODO_WEEK=PERIODO_ANALISIS) %>% 
  sdf_coalesce(1) %>% spark_write_csv("preprocess_data/indicadores_resources_sakai_newdata",mode="append",partition_by = "PERIODO_WEEK")





