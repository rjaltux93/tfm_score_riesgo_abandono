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

PERIODO_ANALISIS <- 6
args = commandArgs(trailingOnly=TRUE)
if (length(args)!=0) {
  PERIODO_ANALISIS <- as.integer(args[1])
}

# Carga de eventos
events <- spark_read_parquet(sc,"./preprocess_data/sst_events_summarize") %>% 
  filter(PERIODO_WEEK<=PERIODO_ANALISIS) %>% 
  mutate(key_id=paste0(SITE_UID,"_",USER_ID)) %>% 
  select(-SITE_UID,-USER_ID) %>% 
  filter(PERIODO_WEEK>0)

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




#################### tcursos_events ####################################
dat_tcursos_events <- events %>%
  select(PERIODO_WEEK,key_id,tcursos_events)

dat_pivot_tcursos_events <- dat_tcursos_events %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tcursos_events = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tcursos_events <-
  proc_pivot_contadores(dat_pivot_tcursos_events,0,'tcursos_events',dat_tcursos_events) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tcursos_events,2,'tcursos_events',dat_tcursos_events),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tcursos_events,3,'tcursos_events',dat_tcursos_events),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tcursos_events,4,'tcursos_events',dat_tcursos_events),by='key_id') %>% 
  sdf_persist()


#################### tt_pres_begin ####################################
dat_tt_pres_begin <- events %>%
  select(PERIODO_WEEK,key_id,tt_pres_begin)

dat_pivot_tt_pres_begin <- dat_tt_pres_begin %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_pres_begin = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_pres_begin <-
  proc_pivot_contadores(dat_pivot_tt_pres_begin,0,'tt_pres_begin',dat_tt_pres_begin) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_pres_begin,2,'tt_pres_begin',dat_tt_pres_begin),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_pres_begin,3,'tt_pres_begin',dat_tt_pres_begin),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_pres_begin,4,'tt_pres_begin',dat_tt_pres_begin),by='key_id') %>% 
  sdf_persist()


#################### tt_basiclti_launch ####################################
dat_tt_basiclti_launch <- events %>%
  select(PERIODO_WEEK,key_id,tt_basiclti_launch)

dat_pivot_tt_basiclti_launch <- dat_tt_basiclti_launch %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_basiclti_launch = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_basiclti_launch <-
  proc_pivot_contadores(dat_pivot_tt_basiclti_launch,0,'tt_basiclti_launch',dat_tt_basiclti_launch) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_basiclti_launch,2,'tt_basiclti_launch',dat_tt_basiclti_launch),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_basiclti_launch,3,'tt_basiclti_launch',dat_tt_basiclti_launch),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_basiclti_launch,4,'tt_basiclti_launch',dat_tt_basiclti_launch),by='key_id') %>% 
  sdf_persist()


#################### tt_lessonbuilder_read ####################################
dat_tt_lessonbuilder_read <- events %>%
  select(PERIODO_WEEK,key_id,tt_lessonbuilder_read)

dat_pivot_tt_lessonbuilder_read <- dat_tt_lessonbuilder_read %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_lessonbuilder_read = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_lessonbuilder_read <-
  proc_pivot_contadores(dat_pivot_tt_lessonbuilder_read,0,'tt_lessonbuilder_read',dat_tt_lessonbuilder_read) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_lessonbuilder_read,2,'tt_lessonbuilder_read',dat_tt_lessonbuilder_read),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_lessonbuilder_read,3,'tt_lessonbuilder_read',dat_tt_lessonbuilder_read),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_lessonbuilder_read,4,'tt_lessonbuilder_read',dat_tt_lessonbuilder_read),by='key_id') %>% 
  sdf_persist()


#################### tt_content_read ####################################
dat_tt_content_read <- events %>%
  select(PERIODO_WEEK,key_id,tt_content_read)

dat_pivot_tt_content_read <- dat_tt_content_read %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_content_read = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_content_read <-
  proc_pivot_contadores(dat_pivot_tt_content_read,0,'tt_content_read',dat_tt_content_read) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_content_read,2,'tt_content_read',dat_tt_content_read),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_content_read,3,'tt_content_read',dat_tt_content_read),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_content_read,4,'tt_content_read',dat_tt_content_read),by='key_id') %>% 
  sdf_persist()


#################### tt_forums_read ####################################
dat_tt_forums_read <- events %>%
  select(PERIODO_WEEK,key_id,tt_forums_read)

dat_pivot_tt_forums_read <- dat_tt_forums_read %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_forums_read = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_forums_read <-
  proc_pivot_contadores(dat_pivot_tt_forums_read,0,'tt_forums_read',dat_tt_forums_read) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_forums_read,2,'tt_forums_read',dat_tt_forums_read),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_forums_read,3,'tt_forums_read',dat_tt_forums_read),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_forums_read,4,'tt_forums_read',dat_tt_forums_read),by='key_id') %>% 
  sdf_persist()


#################### tt_messages_read ####################################
dat_tt_messages_read <- events %>%
  select(PERIODO_WEEK,key_id,tt_messages_read)

dat_pivot_tt_messages_read <- dat_tt_messages_read %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_messages_read = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_messages_read <-
  proc_pivot_contadores(dat_pivot_tt_messages_read,0,'tt_messages_read',dat_tt_messages_read) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_messages_read,2,'tt_messages_read',dat_tt_messages_read),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_messages_read,3,'tt_messages_read',dat_tt_messages_read),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_messages_read,4,'tt_messages_read',dat_tt_messages_read),by='key_id') %>% 
  sdf_persist()


#################### tt_content_new ####################################
dat_tt_content_new <- events %>%
  select(PERIODO_WEEK,key_id,tt_content_new)

dat_pivot_tt_content_new <- dat_tt_content_new %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_content_new = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_content_new <-
  proc_pivot_contadores(dat_pivot_tt_content_new,0,'tt_content_new',dat_tt_content_new) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_content_new,2,'tt_content_new',dat_tt_content_new),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_content_new,3,'tt_content_new',dat_tt_content_new),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_content_new,4,'tt_content_new',dat_tt_content_new),by='key_id') %>% 
  sdf_persist()


#################### tt_sam_assessment_take ####################################
dat_tt_sam_assessment_take <- events %>%
  select(PERIODO_WEEK,key_id,tt_sam_assessment_take)

dat_pivot_tt_sam_assessment_take <- dat_tt_sam_assessment_take %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_sam_assessment_take = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_sam_assessment_take <-
  proc_pivot_contadores(dat_pivot_tt_sam_assessment_take,0,'tt_sam_assessment_take',dat_tt_sam_assessment_take) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_sam_assessment_take,2,'tt_sam_assessment_take',dat_tt_sam_assessment_take),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_sam_assessment_take,3,'tt_sam_assessment_take',dat_tt_sam_assessment_take),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_sam_assessment_take,4,'tt_sam_assessment_take',dat_tt_sam_assessment_take),by='key_id') %>% 
  sdf_persist()


#################### tt_sam_assessment_submit ####################################
dat_tt_sam_assessment_submit <- events %>%
  select(PERIODO_WEEK,key_id,tt_sam_assessment_submit)

dat_pivot_tt_sam_assessment_submit <- dat_tt_sam_assessment_submit %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_sam_assessment_submit = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_sam_assessment_submit <-
  proc_pivot_contadores(dat_pivot_tt_sam_assessment_submit,0,'tt_sam_assessment_submit',dat_tt_sam_assessment_submit) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_sam_assessment_submit,2,'tt_sam_assessment_submit',dat_tt_sam_assessment_submit),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_sam_assessment_submit,3,'tt_sam_assessment_submit',dat_tt_sam_assessment_submit),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_sam_assessment_submit,4,'tt_sam_assessment_submit',dat_tt_sam_assessment_submit),by='key_id') %>% 
  sdf_persist()


#################### tt_asn_submit_submission ####################################
dat_tt_asn_submit_submission <- events %>%
  select(PERIODO_WEEK,key_id,tt_asn_submit_submission)

dat_pivot_tt_asn_submit_submission <- dat_tt_asn_submit_submission %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_asn_submit_submission = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_asn_submit_submission <-
  proc_pivot_contadores(dat_pivot_tt_asn_submit_submission,0,'tt_asn_submit_submission',dat_tt_asn_submit_submission) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_asn_submit_submission,2,'tt_asn_submit_submission',dat_tt_asn_submit_submission),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_asn_submit_submission,3,'tt_asn_submit_submission',dat_tt_asn_submit_submission),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_asn_submit_submission,4,'tt_asn_submit_submission',dat_tt_asn_submit_submission),by='key_id') %>% 
  sdf_persist()


#################### tt_webcontent_read ####################################
dat_tt_webcontent_read <- events %>%
  select(PERIODO_WEEK,key_id,tt_webcontent_read)

dat_pivot_tt_webcontent_read <- dat_tt_webcontent_read %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_webcontent_read = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_webcontent_read <-
  proc_pivot_contadores(dat_pivot_tt_webcontent_read,0,'tt_webcontent_read',dat_tt_webcontent_read) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_webcontent_read,2,'tt_webcontent_read',dat_tt_webcontent_read),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_webcontent_read,3,'tt_webcontent_read',dat_tt_webcontent_read),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_webcontent_read,4,'tt_webcontent_read',dat_tt_webcontent_read),by='key_id') %>% 
  sdf_persist()


#################### tt_messages_reply ####################################
dat_tt_messages_reply <- events %>%
  select(PERIODO_WEEK,key_id,tt_messages_reply)

dat_pivot_tt_messages_reply <- dat_tt_messages_reply %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_messages_reply = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_messages_reply <-
  proc_pivot_contadores(dat_pivot_tt_messages_reply,0,'tt_messages_reply',dat_tt_messages_reply) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_messages_reply,2,'tt_messages_reply',dat_tt_messages_reply),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_messages_reply,3,'tt_messages_reply',dat_tt_messages_reply),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_messages_reply,4,'tt_messages_reply',dat_tt_messages_reply),by='key_id') %>% 
  sdf_persist()


#################### tt_messages_newfolder ####################################
dat_tt_messages_newfolder <- events %>%
  select(PERIODO_WEEK,key_id,tt_messages_newfolder)

dat_pivot_tt_messages_newfolder <- dat_tt_messages_newfolder %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_messages_newfolder = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_messages_newfolder <-
  proc_pivot_contadores(dat_pivot_tt_messages_newfolder,0,'tt_messages_newfolder',dat_tt_messages_newfolder) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_messages_newfolder,2,'tt_messages_newfolder',dat_tt_messages_newfolder),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_messages_newfolder,3,'tt_messages_newfolder',dat_tt_messages_newfolder),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_messages_newfolder,4,'tt_messages_newfolder',dat_tt_messages_newfolder),by='key_id') %>% 
  sdf_persist()


#################### tt_messages_new ####################################
dat_tt_messages_new <- events %>%
  select(PERIODO_WEEK,key_id,tt_messages_new)

dat_pivot_tt_messages_new <- dat_tt_messages_new %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_messages_new = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_messages_new <-
  proc_pivot_contadores(dat_pivot_tt_messages_new,0,'tt_messages_new',dat_tt_messages_new) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_messages_new,2,'tt_messages_new',dat_tt_messages_new),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_messages_new,3,'tt_messages_new',dat_tt_messages_new),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_messages_new,4,'tt_messages_new',dat_tt_messages_new),by='key_id') %>% 
  sdf_persist()


#################### tt_forums_new ####################################
dat_tt_forums_new <- events %>%
  select(PERIODO_WEEK,key_id,tt_forums_new)

dat_pivot_tt_forums_new <- dat_tt_forums_new %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_forums_new = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_forums_new <-
  proc_pivot_contadores(dat_pivot_tt_forums_new,0,'tt_forums_new',dat_tt_forums_new) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_forums_new,2,'tt_forums_new',dat_tt_forums_new),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_forums_new,3,'tt_forums_new',dat_tt_forums_new),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_forums_new,4,'tt_forums_new',dat_tt_forums_new),by='key_id') %>% 
  sdf_persist()


#################### tt_forums_response ####################################
dat_tt_forums_response <- events %>%
  select(PERIODO_WEEK,key_id,tt_forums_response)

dat_pivot_tt_forums_response <- dat_tt_forums_response %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_forums_response = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_forums_response <-
  proc_pivot_contadores(dat_pivot_tt_forums_response,0,'tt_forums_response',dat_tt_forums_response) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_forums_response,2,'tt_forums_response',dat_tt_forums_response),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_forums_response,3,'tt_forums_response',dat_tt_forums_response),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_forums_response,4,'tt_forums_response',dat_tt_forums_response),by='key_id') %>% 
  sdf_persist()


#################### tt_messages_forward ####################################
dat_tt_messages_forward <- events %>%
  select(PERIODO_WEEK,key_id,tt_messages_forward)

dat_pivot_tt_messages_forward <- dat_tt_messages_forward %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_messages_forward = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_messages_forward <-
  proc_pivot_contadores(dat_pivot_tt_messages_forward,0,'tt_messages_forward',dat_tt_messages_forward) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_messages_forward,2,'tt_messages_forward',dat_tt_messages_forward),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_messages_forward,3,'tt_messages_forward',dat_tt_messages_forward),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_messages_forward,4,'tt_messages_forward',dat_tt_messages_forward),by='key_id') %>% 
  sdf_persist()


#################### tt_asn_save_submission ####################################
dat_tt_asn_save_submission <- events %>%
  select(PERIODO_WEEK,key_id,tt_asn_save_submission)

dat_pivot_tt_asn_save_submission <- dat_tt_asn_save_submission %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_asn_save_submission = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_asn_save_submission <-
  proc_pivot_contadores(dat_pivot_tt_asn_save_submission,0,'tt_asn_save_submission',dat_tt_asn_save_submission) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_asn_save_submission,2,'tt_asn_save_submission',dat_tt_asn_save_submission),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_asn_save_submission,3,'tt_asn_save_submission',dat_tt_asn_save_submission),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_asn_save_submission,4,'tt_asn_save_submission',dat_tt_asn_save_submission),by='key_id') %>% 
  sdf_persist()


#################### tt_calendar_revise ####################################
dat_tt_calendar_revise <- events %>%
  select(PERIODO_WEEK,key_id,tt_calendar_revise)

dat_pivot_tt_calendar_revise <- dat_tt_calendar_revise %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_calendar_revise = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_calendar_revise <-
  proc_pivot_contadores(dat_pivot_tt_calendar_revise,0,'tt_calendar_revise',dat_tt_calendar_revise) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_calendar_revise,2,'tt_calendar_revise',dat_tt_calendar_revise),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_calendar_revise,3,'tt_calendar_revise',dat_tt_calendar_revise),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_calendar_revise,4,'tt_calendar_revise',dat_tt_calendar_revise),by='key_id') %>% 
  sdf_persist()


#################### tt_asn_grade_submission ####################################
dat_tt_asn_grade_submission <- events %>%
  select(PERIODO_WEEK,key_id,tt_asn_grade_submission)

dat_pivot_tt_asn_grade_submission <- dat_tt_asn_grade_submission %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_asn_grade_submission = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_asn_grade_submission <-
  proc_pivot_contadores(dat_pivot_tt_asn_grade_submission,0,'tt_asn_grade_submission',dat_tt_asn_grade_submission) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_asn_grade_submission,2,'tt_asn_grade_submission',dat_tt_asn_grade_submission),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_asn_grade_submission,3,'tt_asn_grade_submission',dat_tt_asn_grade_submission),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_asn_grade_submission,4,'tt_asn_grade_submission',dat_tt_asn_grade_submission),by='key_id') %>% 
  sdf_persist()


#################### tt_annc_new ####################################
dat_tt_annc_new <- events %>%
  select(PERIODO_WEEK,key_id,tt_annc_new)

dat_pivot_tt_annc_new <- dat_tt_annc_new %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_annc_new = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_annc_new <-
  proc_pivot_contadores(dat_pivot_tt_annc_new,0,'tt_annc_new',dat_tt_annc_new) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_annc_new,2,'tt_annc_new',dat_tt_annc_new),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_annc_new,3,'tt_annc_new',dat_tt_annc_new),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_annc_new,4,'tt_annc_new',dat_tt_annc_new),by='key_id') %>% 
  sdf_persist()


#################### tt_lessonbuilder_update ####################################
dat_tt_lessonbuilder_update <- events %>%
  select(PERIODO_WEEK,key_id,tt_lessonbuilder_update)

dat_pivot_tt_lessonbuilder_update <- dat_tt_lessonbuilder_update %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_lessonbuilder_update = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_lessonbuilder_update <-
  proc_pivot_contadores(dat_pivot_tt_lessonbuilder_update,0,'tt_lessonbuilder_update',dat_tt_lessonbuilder_update) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_lessonbuilder_update,2,'tt_lessonbuilder_update',dat_tt_lessonbuilder_update),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_lessonbuilder_update,3,'tt_lessonbuilder_update',dat_tt_lessonbuilder_update),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_lessonbuilder_update,4,'tt_lessonbuilder_update',dat_tt_lessonbuilder_update),by='key_id') %>% 
  sdf_persist()


#################### tt_site_upd ####################################
dat_tt_site_upd <- events %>%
  select(PERIODO_WEEK,key_id,tt_site_upd)

dat_pivot_tt_site_upd <- dat_tt_site_upd %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_site_upd = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_site_upd <-
  proc_pivot_contadores(dat_pivot_tt_site_upd,0,'tt_site_upd',dat_tt_site_upd) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_site_upd,2,'tt_site_upd',dat_tt_site_upd),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_site_upd,3,'tt_site_upd',dat_tt_site_upd),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_site_upd,4,'tt_site_upd',dat_tt_site_upd),by='key_id') %>% 
  sdf_persist()


#################### tt_calendar_new ####################################
dat_tt_calendar_new <- events %>%
  select(PERIODO_WEEK,key_id,tt_calendar_new)

dat_pivot_tt_calendar_new <- dat_tt_calendar_new %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_calendar_new = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_calendar_new <-
  proc_pivot_contadores(dat_pivot_tt_calendar_new,0,'tt_calendar_new',dat_tt_calendar_new) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_calendar_new,2,'tt_calendar_new',dat_tt_calendar_new),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_calendar_new,3,'tt_calendar_new',dat_tt_calendar_new),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_calendar_new,4,'tt_calendar_new',dat_tt_calendar_new),by='key_id') %>% 
  sdf_persist()


#################### tt_sam_assessment_take_via_url ####################################
dat_tt_sam_assessment_take_via_url <- events %>%
  select(PERIODO_WEEK,key_id,tt_sam_assessment_take_via_url)

dat_pivot_tt_sam_assessment_take_via_url <- dat_tt_sam_assessment_take_via_url %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_sam_assessment_take_via_url = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_sam_assessment_take_via_url <-
  proc_pivot_contadores(dat_pivot_tt_sam_assessment_take_via_url,0,'tt_sam_assessment_take_via_url',dat_tt_sam_assessment_take_via_url) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_sam_assessment_take_via_url,2,'tt_sam_assessment_take_via_url',dat_tt_sam_assessment_take_via_url),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_sam_assessment_take_via_url,3,'tt_sam_assessment_take_via_url',dat_tt_sam_assessment_take_via_url),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_sam_assessment_take_via_url,4,'tt_sam_assessment_take_via_url',dat_tt_sam_assessment_take_via_url),by='key_id') %>% 
  sdf_persist()


#################### tt_content_delete ####################################
dat_tt_content_delete <- events %>%
  select(PERIODO_WEEK,key_id,tt_content_delete)

dat_pivot_tt_content_delete <- dat_tt_content_delete %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_content_delete = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_content_delete <-
  proc_pivot_contadores(dat_pivot_tt_content_delete,0,'tt_content_delete',dat_tt_content_delete) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_content_delete,2,'tt_content_delete',dat_tt_content_delete),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_content_delete,3,'tt_content_delete',dat_tt_content_delete),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_content_delete,4,'tt_content_delete',dat_tt_content_delete),by='key_id') %>% 
  sdf_persist()


#################### tt_sam_assessment_submit_via_url ####################################
dat_tt_sam_assessment_submit_via_url <- events %>%
  select(PERIODO_WEEK,key_id,tt_sam_assessment_submit_via_url)

dat_pivot_tt_sam_assessment_submit_via_url <- dat_tt_sam_assessment_submit_via_url %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_sam_assessment_submit_via_url = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_sam_assessment_submit_via_url <-
  proc_pivot_contadores(dat_pivot_tt_sam_assessment_submit_via_url,0,'tt_sam_assessment_submit_via_url',dat_tt_sam_assessment_submit_via_url) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_sam_assessment_submit_via_url,2,'tt_sam_assessment_submit_via_url',dat_tt_sam_assessment_submit_via_url),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_sam_assessment_submit_via_url,3,'tt_sam_assessment_submit_via_url',dat_tt_sam_assessment_submit_via_url),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_sam_assessment_submit_via_url,4,'tt_sam_assessment_submit_via_url',dat_tt_sam_assessment_submit_via_url),by='key_id') %>% 
  sdf_persist()


#################### tt_annc_revise_own ####################################
dat_tt_annc_revise_own <- events %>%
  select(PERIODO_WEEK,key_id,tt_annc_revise_own)

dat_pivot_tt_annc_revise_own <- dat_tt_annc_revise_own %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_annc_revise_own = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_annc_revise_own <-
  proc_pivot_contadores(dat_pivot_tt_annc_revise_own,0,'tt_annc_revise_own',dat_tt_annc_revise_own) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_annc_revise_own,2,'tt_annc_revise_own',dat_tt_annc_revise_own),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_annc_revise_own,3,'tt_annc_revise_own',dat_tt_annc_revise_own),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_annc_revise_own,4,'tt_annc_revise_own',dat_tt_annc_revise_own),by='key_id') %>% 
  sdf_persist()


#################### tt_forums_newtopic ####################################
dat_tt_forums_newtopic <- events %>%
  select(PERIODO_WEEK,key_id,tt_forums_newtopic)

dat_pivot_tt_forums_newtopic <- dat_tt_forums_newtopic %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_forums_newtopic = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_forums_newtopic <-
  proc_pivot_contadores(dat_pivot_tt_forums_newtopic,0,'tt_forums_newtopic',dat_tt_forums_newtopic) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_forums_newtopic,2,'tt_forums_newtopic',dat_tt_forums_newtopic),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_forums_newtopic,3,'tt_forums_newtopic',dat_tt_forums_newtopic),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_forums_newtopic,4,'tt_forums_newtopic',dat_tt_forums_newtopic),by='key_id') %>% 
  sdf_persist()


#################### tt_annc_revise_any ####################################
dat_tt_annc_revise_any <- events %>%
  select(PERIODO_WEEK,key_id,tt_annc_revise_any)

dat_pivot_tt_annc_revise_any <- dat_tt_annc_revise_any %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_annc_revise_any = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_annc_revise_any <-
  proc_pivot_contadores(dat_pivot_tt_annc_revise_any,0,'tt_annc_revise_any',dat_tt_annc_revise_any) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_annc_revise_any,2,'tt_annc_revise_any',dat_tt_annc_revise_any),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_annc_revise_any,3,'tt_annc_revise_any',dat_tt_annc_revise_any),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_annc_revise_any,4,'tt_annc_revise_any',dat_tt_annc_revise_any),by='key_id') %>% 
  sdf_persist()


#################### tt_asn_revise_assignment ####################################
dat_tt_asn_revise_assignment <- events %>%
  select(PERIODO_WEEK,key_id,tt_asn_revise_assignment)

dat_pivot_tt_asn_revise_assignment <- dat_tt_asn_revise_assignment %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_asn_revise_assignment = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_asn_revise_assignment <-
  proc_pivot_contadores(dat_pivot_tt_asn_revise_assignment,0,'tt_asn_revise_assignment',dat_tt_asn_revise_assignment) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_asn_revise_assignment,2,'tt_asn_revise_assignment',dat_tt_asn_revise_assignment),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_asn_revise_assignment,3,'tt_asn_revise_assignment',dat_tt_asn_revise_assignment),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_asn_revise_assignment,4,'tt_asn_revise_assignment',dat_tt_asn_revise_assignment),by='key_id') %>% 
  sdf_persist()


#################### tt_messages_delete ####################################
dat_tt_messages_delete <- events %>%
  select(PERIODO_WEEK,key_id,tt_messages_delete)

dat_pivot_tt_messages_delete <- dat_tt_messages_delete %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_messages_delete = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_messages_delete <-
  proc_pivot_contadores(dat_pivot_tt_messages_delete,0,'tt_messages_delete',dat_tt_messages_delete) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_messages_delete,2,'tt_messages_delete',dat_tt_messages_delete),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_messages_delete,3,'tt_messages_delete',dat_tt_messages_delete),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_messages_delete,4,'tt_messages_delete',dat_tt_messages_delete),by='key_id') %>% 
  sdf_persist()


#################### tt_lessonbuilder_delete ####################################
dat_tt_lessonbuilder_delete <- events %>%
  select(PERIODO_WEEK,key_id,tt_lessonbuilder_delete)

dat_pivot_tt_lessonbuilder_delete <- dat_tt_lessonbuilder_delete %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_lessonbuilder_delete = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_lessonbuilder_delete <-
  proc_pivot_contadores(dat_pivot_tt_lessonbuilder_delete,0,'tt_lessonbuilder_delete',dat_tt_lessonbuilder_delete) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_lessonbuilder_delete,2,'tt_lessonbuilder_delete',dat_tt_lessonbuilder_delete),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_lessonbuilder_delete,3,'tt_lessonbuilder_delete',dat_tt_lessonbuilder_delete),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_lessonbuilder_delete,4,'tt_lessonbuilder_delete',dat_tt_lessonbuilder_delete),by='key_id') %>% 
  sdf_persist()


#################### tt_forums_revisetopic ####################################
dat_tt_forums_revisetopic <- events %>%
  select(PERIODO_WEEK,key_id,tt_forums_revisetopic)

dat_pivot_tt_forums_revisetopic <- dat_tt_forums_revisetopic %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_forums_revisetopic = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_forums_revisetopic <-
  proc_pivot_contadores(dat_pivot_tt_forums_revisetopic,0,'tt_forums_revisetopic',dat_tt_forums_revisetopic) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_forums_revisetopic,2,'tt_forums_revisetopic',dat_tt_forums_revisetopic),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_forums_revisetopic,3,'tt_forums_revisetopic',dat_tt_forums_revisetopic),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_forums_revisetopic,4,'tt_forums_revisetopic',dat_tt_forums_revisetopic),by='key_id') %>% 
  sdf_persist()


#################### tt_content_revise ####################################
dat_tt_content_revise <- events %>%
  select(PERIODO_WEEK,key_id,tt_content_revise)

dat_pivot_tt_content_revise <- dat_tt_content_revise %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_content_revise = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_content_revise <-
  proc_pivot_contadores(dat_pivot_tt_content_revise,0,'tt_content_revise',dat_tt_content_revise) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_content_revise,2,'tt_content_revise',dat_tt_content_revise),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_content_revise,3,'tt_content_revise',dat_tt_content_revise),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_content_revise,4,'tt_content_revise',dat_tt_content_revise),by='key_id') %>% 
  sdf_persist()


#################### tt_lessonbuilder_create ####################################
dat_tt_lessonbuilder_create <- events %>%
  select(PERIODO_WEEK,key_id,tt_lessonbuilder_create)

dat_pivot_tt_lessonbuilder_create <- dat_tt_lessonbuilder_create %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_lessonbuilder_create = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_lessonbuilder_create <-
  proc_pivot_contadores(dat_pivot_tt_lessonbuilder_create,0,'tt_lessonbuilder_create',dat_tt_lessonbuilder_create) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_lessonbuilder_create,2,'tt_lessonbuilder_create',dat_tt_lessonbuilder_create),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_lessonbuilder_create,3,'tt_lessonbuilder_create',dat_tt_lessonbuilder_create),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_lessonbuilder_create,4,'tt_lessonbuilder_create',dat_tt_lessonbuilder_create),by='key_id') %>% 
  sdf_persist()


#################### tt_sitestats_view ####################################
dat_tt_sitestats_view <- events %>%
  select(PERIODO_WEEK,key_id,tt_sitestats_view)

dat_pivot_tt_sitestats_view <- dat_tt_sitestats_view %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_sitestats_view = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_sitestats_view <-
  proc_pivot_contadores(dat_pivot_tt_sitestats_view,0,'tt_sitestats_view',dat_tt_sitestats_view) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_sitestats_view,2,'tt_sitestats_view',dat_tt_sitestats_view),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_sitestats_view,3,'tt_sitestats_view',dat_tt_sitestats_view),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_sitestats_view,4,'tt_sitestats_view',dat_tt_sitestats_view),by='key_id') %>% 
  sdf_persist()


#################### tt_forums_delete ####################################
dat_tt_forums_delete <- events %>%
  select(PERIODO_WEEK,key_id,tt_forums_delete)

dat_pivot_tt_forums_delete <- dat_tt_forums_delete %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_forums_delete = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_forums_delete <-
  proc_pivot_contadores(dat_pivot_tt_forums_delete,0,'tt_forums_delete',dat_tt_forums_delete) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_forums_delete,2,'tt_forums_delete',dat_tt_forums_delete),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_forums_delete,3,'tt_forums_delete',dat_tt_forums_delete),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_forums_delete,4,'tt_forums_delete',dat_tt_forums_delete),by='key_id') %>% 
  sdf_persist()


#################### tt_forums_deleteforum ####################################
dat_tt_forums_deleteforum <- events %>%
  select(PERIODO_WEEK,key_id,tt_forums_deleteforum)

dat_pivot_tt_forums_deleteforum <- dat_tt_forums_deleteforum %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_forums_deleteforum = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_forums_deleteforum <-
  proc_pivot_contadores(dat_pivot_tt_forums_deleteforum,0,'tt_forums_deleteforum',dat_tt_forums_deleteforum) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_forums_deleteforum,2,'tt_forums_deleteforum',dat_tt_forums_deleteforum),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_forums_deleteforum,3,'tt_forums_deleteforum',dat_tt_forums_deleteforum),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_forums_deleteforum,4,'tt_forums_deleteforum',dat_tt_forums_deleteforum),by='key_id') %>% 
  sdf_persist()


#################### tt_forums_deletetopic ####################################
dat_tt_forums_deletetopic <- events %>%
  select(PERIODO_WEEK,key_id,tt_forums_deletetopic)

dat_pivot_tt_forums_deletetopic <- dat_tt_forums_deletetopic %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_forums_deletetopic = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_forums_deletetopic <-
  proc_pivot_contadores(dat_pivot_tt_forums_deletetopic,0,'tt_forums_deletetopic',dat_tt_forums_deletetopic) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_forums_deletetopic,2,'tt_forums_deletetopic',dat_tt_forums_deletetopic),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_forums_deletetopic,3,'tt_forums_deletetopic',dat_tt_forums_deletetopic),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_forums_deletetopic,4,'tt_forums_deletetopic',dat_tt_forums_deletetopic),by='key_id') %>% 
  sdf_persist()


#################### tt_annc_delete_own ####################################
dat_tt_annc_delete_own <- events %>%
  select(PERIODO_WEEK,key_id,tt_annc_delete_own)

dat_pivot_tt_annc_delete_own <- dat_tt_annc_delete_own %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_annc_delete_own = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_annc_delete_own <-
  proc_pivot_contadores(dat_pivot_tt_annc_delete_own,0,'tt_annc_delete_own',dat_tt_annc_delete_own) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_annc_delete_own,2,'tt_annc_delete_own',dat_tt_annc_delete_own),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_annc_delete_own,3,'tt_annc_delete_own',dat_tt_annc_delete_own),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_annc_delete_own,4,'tt_annc_delete_own',dat_tt_annc_delete_own),by='key_id') %>% 
  sdf_persist()


#################### tt_sitestats_report_new ####################################
dat_tt_sitestats_report_new <- events %>%
  select(PERIODO_WEEK,key_id,tt_sitestats_report_new)

dat_pivot_tt_sitestats_report_new <- dat_tt_sitestats_report_new %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_sitestats_report_new = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_sitestats_report_new <-
  proc_pivot_contadores(dat_pivot_tt_sitestats_report_new,0,'tt_sitestats_report_new',dat_tt_sitestats_report_new) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_sitestats_report_new,2,'tt_sitestats_report_new',dat_tt_sitestats_report_new),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_sitestats_report_new,3,'tt_sitestats_report_new',dat_tt_sitestats_report_new),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_sitestats_report_new,4,'tt_sitestats_report_new',dat_tt_sitestats_report_new),by='key_id') %>% 
  sdf_persist()


#################### tt_sitestats_report_view ####################################
dat_tt_sitestats_report_view <- events %>%
  select(PERIODO_WEEK,key_id,tt_sitestats_report_view)

dat_pivot_tt_sitestats_report_view <- dat_tt_sitestats_report_view %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_sitestats_report_view = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_sitestats_report_view <-
  proc_pivot_contadores(dat_pivot_tt_sitestats_report_view,0,'tt_sitestats_report_view',dat_tt_sitestats_report_view) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_sitestats_report_view,2,'tt_sitestats_report_view',dat_tt_sitestats_report_view),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_sitestats_report_view,3,'tt_sitestats_report_view',dat_tt_sitestats_report_view),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_sitestats_report_view,4,'tt_sitestats_report_view',dat_tt_sitestats_report_view),by='key_id') %>% 
  sdf_persist()


#################### tt_asn_delete_assignment ####################################
dat_tt_asn_delete_assignment <- events %>%
  select(PERIODO_WEEK,key_id,tt_asn_delete_assignment)

dat_pivot_tt_asn_delete_assignment <- dat_tt_asn_delete_assignment %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_asn_delete_assignment = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_asn_delete_assignment <-
  proc_pivot_contadores(dat_pivot_tt_asn_delete_assignment,0,'tt_asn_delete_assignment',dat_tt_asn_delete_assignment) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_asn_delete_assignment,2,'tt_asn_delete_assignment',dat_tt_asn_delete_assignment),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_asn_delete_assignment,3,'tt_asn_delete_assignment',dat_tt_asn_delete_assignment),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_asn_delete_assignment,4,'tt_asn_delete_assignment',dat_tt_asn_delete_assignment),by='key_id') %>% 
  sdf_persist()


#################### tt_webcontent_revise ####################################
dat_tt_webcontent_revise <- events %>%
  select(PERIODO_WEEK,key_id,tt_webcontent_revise)

dat_pivot_tt_webcontent_revise <- dat_tt_webcontent_revise %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_webcontent_revise = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_webcontent_revise <-
  proc_pivot_contadores(dat_pivot_tt_webcontent_revise,0,'tt_webcontent_revise',dat_tt_webcontent_revise) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_webcontent_revise,2,'tt_webcontent_revise',dat_tt_webcontent_revise),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_webcontent_revise,3,'tt_webcontent_revise',dat_tt_webcontent_revise),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_webcontent_revise,4,'tt_webcontent_revise',dat_tt_webcontent_revise),by='key_id') %>% 
  sdf_persist()


#################### tt_asn_new_assignment ####################################
dat_tt_asn_new_assignment <- events %>%
  select(PERIODO_WEEK,key_id,tt_asn_new_assignment)

dat_pivot_tt_asn_new_assignment <- dat_tt_asn_new_assignment %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_asn_new_assignment = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_asn_new_assignment <-
  proc_pivot_contadores(dat_pivot_tt_asn_new_assignment,0,'tt_asn_new_assignment',dat_tt_asn_new_assignment) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_asn_new_assignment,2,'tt_asn_new_assignment',dat_tt_asn_new_assignment),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_asn_new_assignment,3,'tt_asn_new_assignment',dat_tt_asn_new_assignment),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_asn_new_assignment,4,'tt_asn_new_assignment',dat_tt_asn_new_assignment),by='key_id') %>% 
  sdf_persist()


#################### tt_sam_total_score_update ####################################
dat_tt_sam_total_score_update <- events %>%
  select(PERIODO_WEEK,key_id,tt_sam_total_score_update)

dat_pivot_tt_sam_total_score_update <- dat_tt_sam_total_score_update %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_sam_total_score_update = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_sam_total_score_update <-
  proc_pivot_contadores(dat_pivot_tt_sam_total_score_update,0,'tt_sam_total_score_update',dat_tt_sam_total_score_update) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_sam_total_score_update,2,'tt_sam_total_score_update',dat_tt_sam_total_score_update),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_sam_total_score_update,3,'tt_sam_total_score_update',dat_tt_sam_total_score_update),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_sam_total_score_update,4,'tt_sam_total_score_update',dat_tt_sam_total_score_update),by='key_id') %>% 
  sdf_persist()


#################### tt_sam_pubassessment_confirm_edit ####################################
dat_tt_sam_pubassessment_confirm_edit <- events %>%
  select(PERIODO_WEEK,key_id,tt_sam_pubassessment_confirm_edit)

dat_pivot_tt_sam_pubassessment_confirm_edit <- dat_tt_sam_pubassessment_confirm_edit %>% dplyr::group_by(key_id) %>%
  sdf_pivot(key_id ~ PERIODO_WEEK,fun.aggregate = list(tt_sam_pubassessment_confirm_edit = 'sum')) %>%
  select(c(1),last_col(4):last_col(0)) %>%
  na.replace(0) %>% sdf_persist()

cont_tt_sam_pubassessment_confirm_edit <-
  proc_pivot_contadores(dat_pivot_tt_sam_pubassessment_confirm_edit,0,'tt_sam_pubassessment_confirm_edit',dat_tt_sam_pubassessment_confirm_edit) %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_sam_pubassessment_confirm_edit,2,'tt_sam_pubassessment_confirm_edit',dat_tt_sam_pubassessment_confirm_edit),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_sam_pubassessment_confirm_edit,3,'tt_sam_pubassessment_confirm_edit',dat_tt_sam_pubassessment_confirm_edit),by='key_id') %>%
  left_join(.,proc_pivot_contadores(dat_pivot_tt_sam_pubassessment_confirm_edit,4,'tt_sam_pubassessment_confirm_edit',dat_tt_sam_pubassessment_confirm_edit),by='key_id') %>% 
  sdf_persist()


dfinal1 <- 
  events %>% 
  filter(PERIODO_WEEK==PERIODO_ANALISIS) %>% 
  select(PERIODO_WEEK,key_id) %>% distinct() %>% 
  full_join(.,cont_tcursos_events,by='key_id') %>% 
  full_join(.,cont_tt_pres_begin,by='key_id') %>% 
  full_join(.,cont_tt_basiclti_launch,by='key_id') %>% 
  full_join(.,cont_tt_lessonbuilder_read,by='key_id') %>% 
  full_join(.,cont_tt_content_read,by='key_id') %>% 
  full_join(.,cont_tt_forums_read,by='key_id') %>% 
  full_join(.,cont_tt_messages_read,by='key_id') %>% 
  full_join(.,cont_tt_content_new,by='key_id') %>% 
  full_join(.,cont_tt_sam_assessment_take,by='key_id') %>% 
  full_join(.,cont_tt_sam_assessment_submit,by='key_id') %>% 
  full_join(.,cont_tt_asn_submit_submission,by='key_id') %>% 
  full_join(.,cont_tt_webcontent_read,by='key_id') %>% 
  full_join(.,cont_tt_messages_reply,by='key_id') %>% 
  full_join(.,cont_tt_messages_newfolder,by='key_id') %>% 
  full_join(.,cont_tt_messages_new,by='key_id') %>% 
  full_join(.,cont_tt_forums_new,by='key_id') 

dfinal2 <- 
  full_join(cont_tt_forums_response,
            cont_tt_messages_forward,by='key_id') %>% 
  full_join(.,cont_tt_asn_save_submission,by='key_id') %>% 
  full_join(.,cont_tt_calendar_revise,by='key_id') %>% 
  full_join(.,cont_tt_asn_grade_submission,by='key_id') %>% 
  full_join(.,cont_tt_annc_new,by='key_id') %>% 
  full_join(.,cont_tt_lessonbuilder_update,by='key_id') %>% 
  full_join(.,cont_tt_site_upd,by='key_id') %>% 
  full_join(.,cont_tt_calendar_new,by='key_id') %>% 
  full_join(.,cont_tt_sam_assessment_take_via_url,by='key_id') %>% 
  full_join(.,cont_tt_content_delete,by='key_id') %>% 
  full_join(.,cont_tt_sam_assessment_submit_via_url,by='key_id') %>% 
  full_join(.,cont_tt_annc_revise_own,by='key_id') %>% 
  full_join(.,cont_tt_forums_newtopic,by='key_id') %>% 
  full_join(.,cont_tt_annc_revise_any,by='key_id') %>% 
  full_join(.,cont_tt_asn_revise_assignment,by='key_id') %>% 
  full_join(.,cont_tt_messages_delete,by='key_id') %>% 
  full_join(.,cont_tt_lessonbuilder_delete,by='key_id') %>% 
  full_join(.,cont_tt_forums_revisetopic,by='key_id') %>% 
  full_join(.,cont_tt_content_revise,by='key_id') 

dfinal3 <- 
  full_join(cont_tt_lessonbuilder_create,
            cont_tt_sitestats_view,by='key_id') %>% 
  full_join(.,cont_tt_forums_delete,by='key_id') %>% 
  full_join(.,cont_tt_forums_deleteforum,by='key_id') %>% 
  full_join(.,cont_tt_forums_deletetopic,by='key_id') %>% 
  full_join(.,cont_tt_annc_delete_own,by='key_id') %>% 
  full_join(.,cont_tt_sitestats_report_new,by='key_id') %>% 
  full_join(.,cont_tt_sitestats_report_view,by='key_id') %>% 
  full_join(.,cont_tt_asn_delete_assignment,by='key_id') %>% 
  full_join(.,cont_tt_webcontent_revise,by='key_id') %>% 
  full_join(.,cont_tt_asn_new_assignment,by='key_id') %>% 
  full_join(.,cont_tt_sam_total_score_update,by='key_id') %>% 
  full_join(.,cont_tt_sam_pubassessment_confirm_edit,by='key_id')



dfinal1 %>% spark_write_parquet(.,"indicadores_eventos_1", mode="overwrite")
dfinal2 %>% spark_write_parquet(.,"indicadores_eventos_2", mode="overwrite")
dfinal3 %>% spark_write_parquet(.,"indicadores_eventos_3", mode="overwrite")

spark_read_parquet(sc,"indicadores_eventos_1") %>% 
  left_join(.,spark_read_parquet(sc,"indicadores_eventos_2"),by="key_id") %>% 
  left_join(.,spark_read_parquet(sc,"indicadores_eventos_3"),by="key_id") %>% 
  mutate(PERIODO_WEEK=PERIODO_ANALISIS) %>% 
  sdf_coalesce(1) %>% spark_write_csv("preprocess_data/indicadores_eventos_sakai_newdata",mode="append",partition_by = "PERIODO_WEEK")









