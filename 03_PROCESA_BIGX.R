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
PERIODO_ANALISIS <- 6

args = commandArgs(trailingOnly=TRUE)
if (length(args)!=0) {
  PERIODO_ANALISIS <- as.integer(args[1])
}

# Carga de estudiantes y programas seleccionadados
sites_users <- spark_read_csv(sc,"./preprocess_data/students_marca_abandono_xnotas.csv") %>% 
  mutate(key_id=paste0(SITE_UID,"_",STUDENT_ID)) %>% 
  select(-SITE_UID,-STUDENT_ID,-tprogramas,-marca_extraordinaria,-marca_reprobado)

################################################################################
# Carga de eventos
events <- spark_read_csv(sc,"./preprocess_data/indicadores_eventos_sakai_newdata") %>% 
  filter(PERIODO_WEEK==PERIODO_ANALISIS) %>% 
  select(-PERIODO_WEEK)

################################################################################
# Carga de recursos
resources <- spark_read_csv(sc,"./preprocess_data/indicadores_resources_sakai_newdata")%>% 
  filter(PERIODO_WEEK==PERIODO_ANALISIS)%>% 
  select(-PERIODO_WEEK)

################################################################################
# Carga de logins
userstats <- spark_read_csv(sc,"./preprocess_data/indicadores_userstats_sakai_newdata")%>% 
  filter(PERIODO_WEEK==PERIODO_ANALISIS)%>% 
  select(-PERIODO_WEEK)


bigx <- 
sites_users %>% 
  left_join(events,by="key_id") %>% 
  left_join(resources,by="key_id") %>% 
  left_join(userstats,by="key_id")


#bigx %>% sdf_coalesce(1) %>% spark_write_parquet(.,"bigx_users_sakai_test",mode="overwrite")
bigx %>% sdf_coalesce(1) %>% spark_write_csv(.,paste0("bigx_users_sakai_newdata_",PERIODO_ANALISIS),mode="overwrite")
################################################################################



