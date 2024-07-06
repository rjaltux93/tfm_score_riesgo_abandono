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

setwd("D:/Dataset/Unir/data_tfm")

PATH_OUT <- "./TFM_PROD/preprocess_data"

USER_ID_EX <- 'af8544ab-d7dd-4f9b-bfb1-0150879c8f02'

## 1. SAKAI_SITE: Seleccion de programas
sakai_site <- 
  spark_read_parquet(sc,path = "./Datos_Sakai_Parquet/sakai_site") %>% 
  mutate(PERIODO=as.integer(date_format(CREATEDON,"yyyyMM")),
         split_site=split(SITE_ID,"-"),
         split_title=split(TITLE," "),
         TITLE= as.character(encode(TITLE,"windows-1252")),
         TIPO_ESTUDIO = case_when(rlike(toupper(TITLE),"^MÁSTER*") ~ "MAESTRIA",
                                  rlike(toupper(TITLE),"^GRADO*") ~ "GRADO",
                                  T ~ "OTROS"),
         TIPO_SITIO = case_when(contains(TITLE,"Trabajo Fin de Máster") ~ "C. TFM",
                                contains(SITE_ID,"-000") ~ "A. Programa",
                                contains(SITE_ID,"PER") ~ "B. Asignatura",
                                T ~ "D. Otros")) %>% 
  sdf_separate_column(column = "split_site", into = c("SID","IDMASTER","IDASIGNATURA","IDPARALELO")) %>% 
  mutate(SITE_UID = paste0(SID,"-",IDMASTER,"-",IDPARALELO),
         IDMASTER = as.integer(IDMASTER),
         size = array_size(split_title)) %>%
  mutate(PERIODO_ESCOLAR = element_at(split_title, size)) %>% 
  filter(
    PUBLISHED == 1 & TYPE=="course" & contains(SITE_ID,"PER") &
      !rlike(TITLE,"^Demo*")  & !is.na(SITE_UID) & 
      !rlike(toupper(SITE_ID),"^DEMO*")) %>% 
  select(TIPO_ESTUDIO,SITE_UID,PERIODO_ESCOLAR,SITE_ID,IDMASTER,TIPO_SITIO,TITLE,CREATEDON,PERIODO)

# ESCOGER EL MES CON MÁS MAESTRÍAS EN CURSO
# sakai_site %>% filter(TIPO_ESTUDIO=="MAESTRIA") %>% 
#   group_by(PERIODO_ESCOLAR,PERIODO) %>% 
#   count() %>% arrange(desc(PERIODO))

site_select <- 
  sakai_site %>% filter(TIPO_ESTUDIO=="MAESTRIA" & PERIODO <= 202202) %>% 
  select(SITE_UID) %>% distinct() 

sakai_site <- 
  left_join(site_select,sakai_site,by="SITE_UID") %>% 
  select(-TIPO_ESTUDIO) %>% 
  group_by(PERIODO_ESCOLAR,SITE_UID) %>% 
  mutate(tregistros=n(),
         FECHA_INICIO=min(CREATEDON[TIPO_SITIO=="A. Programa"]),
         tprogramas=sum(ifelse(TIPO_SITIO=="A. Programa",1,0)),
         tasignaturas=sum(ifelse(TIPO_SITIO=="B. Asignatura",1,0)),
         ttfm=sum(ifelse(TIPO_SITIO=="C. TFM",1,0))) %>% ungroup() %>% 
  filter(tprogramas>0 & tasignaturas>4 & ttfm==1) %>% arrange(PERIODO_ESCOLAR)


sakai_site %>%
  sdf_collect() %>%
  write_excel_csv(.,"./TFM_PROD/preprocess_data/sakai_site_clean.csv")



# {r 2. SAKAI_SITE_USER: Seleccion de alumnos}
sakai_site_user <- 
  spark_read_parquet(sc,path = "./Datos_Sakai_Parquet/sakai_site_user") %>%  
  mutate(split_site=split(SITE_ID,"-"),) %>% 
  sdf_separate_column(column = "split_site", into = c("SID","IDMASTER","IDASIGNATURA","IDPARALELO")) %>% 
  mutate(SITE_UID = paste0(SID,"-",IDMASTER,"-",IDPARALELO),
         IDMASTER = as.integer(IDMASTER)) %>% #filter(PERMISSION==1) %>%
  left_join(sakai_site %>% select(SITE_UID,FECHA_INICIO,SITE_ID,TIPO_SITIO)%>% distinct(),.,by=c("SITE_UID","SITE_ID")) %>% 
  left_join(.,
            spark_read_parquet(sc,path = "./Datos_Sakai_Parquet/sakai_realm_rl_gr") %>% 
              left_join(.,spark_read_parquet(sc,path = "./Datos_Sakai_Parquet/sakai_realm_role"),by="ROLE_KEY") %>% 
              filter(ACTIVE==1 & ROLE_NAME=="Alumno") %>% distinct_at(.vars = c("USER_ID"),.keep_all = T) %>% 
              select(USER_ID,ROLE_NAME),
            by="USER_ID") %>% filter(ROLE_NAME=="Alumno") %>% 
  select(SITE_UID,FECHA_INICIO,SITE_ID,TIPO_SITIO,USER_ID)


# TOTAL ESTUDIANTES POR PROGRAMA
# sakai_site_user %>% 
#   group_by(SITE_UID) %>% 
#   summarise(total_alumnos=n_distinct(USER_ID)) %>% arrange(desc(total_alumnos))

sakai_site_user %>%
  distinct_at(.vars = c("SITE_UID","USER_ID")) %>%
  sdf_collect() %>%
  write_excel_csv(.,"./TFM_PROD/preprocess_data/sakai_site_user_clean.csv")

# TOTAL DE ESTUDIANTES 1977
sakai_users_select <- 
  sakai_site_user %>%
  group_by(SITE_UID,USER_ID) %>% 
  summarise(tsite_programas=sum(ifelse(TIPO_SITIO=="A. Programa",1,0)),
            tsite_asignaturas=sum(ifelse(TIPO_SITIO=="B. Asignatura",1,0)),
            tsite_tfms=sum(ifelse(TIPO_SITIO=="C. TFM",1,0)),
            tsite_otros=sum(ifelse(TIPO_SITIO=="D. Otros",1,0))) 

sakai_users_select %>%
  sdf_collect() %>%
  write_excel_csv(.,"./TFM_PROD/preprocess_data/sakai_users_select_clean.csv")

sakai_users_select %>% filter(USER_ID==USER_ID_EX)



# {r 3. GRADEBOOK: Selección de libros de calificaciones}
gb_gradebook_t <- 
  spark_read_parquet(sc,path = "./Datos_Sakai_Parquet/gb_gradebook_t") %>% 
  mutate(split_site=split(GRADEBOOK_UID,"-")) %>% 
  sdf_separate_column(column = "split_site", into = c("SID","IDMASTER","IDASIGNATURA","IDPARALELO")) %>% 
  mutate(SITE_UID = paste0(SID,"-",IDMASTER,"-",IDPARALELO),
         IDMASTER = as.integer(IDMASTER),
         GRADEBOOK_ID=ID) %>% 
  left_join(sakai_site %>% select(SITE_UID,SITE_ID,TIPO_SITIO) %>% distinct(),.,by=c("SITE_UID","SITE_ID"="GRADEBOOK_UID")) %>% 
  select(GRADEBOOK_ID,SITE_UID, SITE_ID,TIPO_SITIO, CATEGORY_TYPE, SELECTED_GRADE_MAPPING_ID) %>% 
  filter(!is.na(GRADEBOOK_ID))



# {r 4. GRADABLE OBJECT: Selección de items a calificar}
gb_gradable_object_t <- 
  spark_read_parquet(sc,path = "./Datos_Sakai_Parquet/gb_gradable_object_t") %>%
  mutate(NAME= as.character(encode(NAME,"windows-1252"))) %>% 
  rename(GRADABLE_OBJECT_ID=ID) %>% 
  select(GRADABLE_OBJECT_ID,GRADEBOOK_ID, OBJECT_TYPE_ID,DUE_DATE, NAME, POINTS_POSSIBLE, CATEGORY_ID) %>% 
  left_join(gb_gradebook_t,.,by="GRADEBOOK_ID")


# {r 5. GRADE RECORD: Selección de notas de los estudiantes}
gb_grade_record_t <- 
  spark_read_parquet(sc,path = "./Datos_Sakai_Parquet/gb_grade_record_t") %>% 
  select(GRADABLE_OBJECT_ID,STUDENT_ID,GRADER_ID,POINTS_EARNED,DATE_RECORDED,POINTS_EARNED_EXTRA) %>% 
  left_join(gb_gradable_object_t,.,by="GRADABLE_OBJECT_ID")  %>% 
  mutate(TIPO_NOTA=case_when(rlike(toupper(NAME),"ACTIVIDAD|TRABAJO|LABORATORIO")~"ACTIVIDADES",
                             rlike(toupper(NAME),"TEST")~"TEST",
                             rlike(toupper(NAME),"MEMORIA|INFORME|PRÁCTICAS")~"PRACTICAS",
                             rlike(toupper(NAME),"ASISTENCIA|CLASE|SESIÓN|DIRECTO")~"ASISTENCIA",
                             rlike(toupper(NAME),"EXAMEN")~"EXAMEN",
                             rlike(toupper(NAME),"COURSE GRADE")~"NOTA_FINAL",
                             T ~ "OTROS"),
         RATIO_POINTS_EARNED = round(POINTS_EARNED/POINTS_POSSIBLE,2)
  ) %>% filter(!is.na(STUDENT_ID))

marca <- 
  gb_grade_record_t %>% 
  filter(TIPO_NOTA=="NOTA_FINAL" & CATEGORY_TYPE!=1) %>% 
  select(SITE_UID,SITE_ID,STUDENT_ID,TIPO_NOTA,POINTS_POSSIBLE,POINTS_EARNED,POINTS_EARNED_EXTRA) %>% 
  mutate(marca_extraordinaria=if_else(POINTS_EARNED<50 | is.na(POINTS_EARNED),1,0),
         marca_reprobado=if_else( (POINTS_EARNED<50 | is.na(POINTS_EARNED)) & (POINTS_EARNED_EXTRA<50 | is.na(POINTS_EARNED_EXTRA)),1,0),
         marca_abandono=case_when( is.na(POINTS_EARNED) & is.na(POINTS_EARNED_EXTRA) ~ 1,
                                   POINTS_EARNED<50 & is.na(POINTS_EARNED_EXTRA) ~ 1,
                                   is.na(POINTS_EARNED) & POINTS_EARNED_EXTRA <50 ~ 1,
                                   POINTS_EARNED<25 & POINTS_EARNED_EXTRA <25 ~ 1,
                                   T ~ 0),
         detalle_abandono=case_when( is.na(POINTS_EARNED) & is.na(POINTS_EARNED_EXTRA) ~ "1",
                                     POINTS_EARNED<50 & is.na(POINTS_EARNED_EXTRA) ~ "2",
                                     is.na(POINTS_EARNED) & POINTS_EARNED_EXTRA <50 ~ "3",
                                     POINTS_EARNED<25 & POINTS_EARNED_EXTRA <25 ~ "4",
                                     T ~ NA)) %>% 
  #filter(STUDENT_ID == "1b24865c-ce30-43b2-96f6-333372d9e4ee")
  group_by(SITE_UID,STUDENT_ID) %>% 
  summarise(tprogramas=n_distinct(SITE_UID),
            tasignaturas=n_distinct(SITE_ID),
            marca_abandono=max(marca_abandono),
            marca_extraordinaria=max(marca_extraordinaria),
            marca_reprobado=max(marca_reprobado),
            .groups = "drop"
  )

marca %>% sdf_collect() %>% write_excel_csv(.,"./TFM_PROD/preprocess_data/students_marca_abandono_xnotas.csv")

sst_events <- 
  spark_read_parquet(sc,path = "./Datos_Sakai_Parquet/sst_events") %>%  
  mutate(split_site=split(SITE_ID,"-")) %>% 
  sdf_separate_column(column = "split_site", into = c("SID","IDMASTER","IDASIGNATURA","IDPARALELO")) %>% 
  mutate(SITE_UID = paste0(SID,"-",IDMASTER,"-",IDPARALELO),
         IDMASTER = as.integer(IDMASTER),
         PERIODO=as.integer(date_format(EVENT_DATE,"yyyyMM")),
         nweek=date_format(EVENT_DATE,'W'),
         dweek=date_format(EVENT_DATE,'E'),
         fds=if_else(dweek%in%c("Sun","Sat"),1,0)) %>% 
  left_join(sakai_site_user %>% distinct(),.,by=c("SITE_UID","SITE_ID","USER_ID")) %>% 
  mutate(PERIODO_WEEK = as.integer((datediff(EVENT_DATE,FECHA_INICIO)/7) +1)) %>% filter(!is.na(EVENT_DATE)) %>% 
  group_by(SITE_UID,PERIODO_WEEK,USER_ID) %>% 
  summarise(tcursos_events=n_distinct(SITE_ID), 
            tt_pres_begin=sum(EVENT_COUNT[EVENT_ID=="pres.begin"]),
            tt_basiclti_launch=sum(EVENT_COUNT[EVENT_ID=="basiclti.launch"]),
            tt_lessonbuilder_read=sum(EVENT_COUNT[EVENT_ID=="lessonbuilder.read"]),
            tt_content_read=sum(EVENT_COUNT[EVENT_ID=="content.read"]),
            tt_forums_read=sum(EVENT_COUNT[EVENT_ID=="forums.read"]),
            tt_messages_read=sum(EVENT_COUNT[EVENT_ID=="messages.read"]),
            tt_content_new=sum(EVENT_COUNT[EVENT_ID=="content.new"]),
            tt_sam_assessment_take=sum(EVENT_COUNT[EVENT_ID=="sam.assessment.take"]),
            tt_sam_assessment_submit=sum(EVENT_COUNT[EVENT_ID=="sam.assessment.submit"]),
            tt_asn_submit_submission=sum(EVENT_COUNT[EVENT_ID=="asn.submit.submission"]),
            tt_webcontent_read=sum(EVENT_COUNT[EVENT_ID=="webcontent.read"]),
            tt_messages_reply=sum(EVENT_COUNT[EVENT_ID=="messages.reply"]),
            tt_messages_newfolder=sum(EVENT_COUNT[EVENT_ID=="messages.newfolder"]),
            tt_messages_new=sum(EVENT_COUNT[EVENT_ID=="messages.new"]),
            tt_forums_new=sum(EVENT_COUNT[EVENT_ID=="forums.new"]),
            tt_forums_response=sum(EVENT_COUNT[EVENT_ID=="forums.response"]),
            tt_messages_forward=sum(EVENT_COUNT[EVENT_ID=="messages.forward"]),
            tt_asn_save_submission=sum(EVENT_COUNT[EVENT_ID=="asn.save.submission"]),
            tt_calendar_revise=sum(EVENT_COUNT[EVENT_ID=="calendar.revise"]),
            tt_asn_grade_submission=sum(EVENT_COUNT[EVENT_ID=="asn.grade.submission"]),
            tt_annc_new=sum(EVENT_COUNT[EVENT_ID=="annc.new"]),
            tt_lessonbuilder_update=sum(EVENT_COUNT[EVENT_ID=="lessonbuilder.update"]),
            tt_site_upd=sum(EVENT_COUNT[EVENT_ID=="site.upd"]),
            tt_calendar_new=sum(EVENT_COUNT[EVENT_ID=="calendar.new"]),
            tt_sam_assessment_take_via_url=sum(EVENT_COUNT[EVENT_ID=="sam.assessment.take.via_url"]),
            tt_content_delete=sum(EVENT_COUNT[EVENT_ID=="content.delete"]),
            tt_sam_assessment_submit_via_url=sum(EVENT_COUNT[EVENT_ID=="sam.assessment.submit.via_url"]),
            tt_annc_revise_own=sum(EVENT_COUNT[EVENT_ID=="annc.revise.own"]),
            tt_forums_newtopic=sum(EVENT_COUNT[EVENT_ID=="forums.newtopic"]),
            tt_annc_revise_any=sum(EVENT_COUNT[EVENT_ID=="annc.revise.any"]),
            tt_asn_revise_assignment=sum(EVENT_COUNT[EVENT_ID=="asn.revise.assignment"]),
            tt_messages_delete=sum(EVENT_COUNT[EVENT_ID=="messages.delete"]),
            tt_lessonbuilder_delete=sum(EVENT_COUNT[EVENT_ID=="lessonbuilder.delete"]),
            tt_forums_revisetopic=sum(EVENT_COUNT[EVENT_ID=="forums.revisetopic"]),
            tt_content_revise=sum(EVENT_COUNT[EVENT_ID=="content.revise"]),
            tt_lessonbuilder_create=sum(EVENT_COUNT[EVENT_ID=="lessonbuilder.create"]),
            tt_sitestats_view=sum(EVENT_COUNT[EVENT_ID=="sitestats.view"]),
            tt_forums_delete=sum(EVENT_COUNT[EVENT_ID=="forums.delete"]),
            tt_forums_deleteforum=sum(EVENT_COUNT[EVENT_ID=="forums.deleteforum"]),
            tt_forums_deletetopic=sum(EVENT_COUNT[EVENT_ID=="forums.deletetopic"]),
            tt_annc_delete_own=sum(EVENT_COUNT[EVENT_ID=="annc.delete.own"]),
            tt_sitestats_report_new=sum(EVENT_COUNT[EVENT_ID=="sitestats.report.new"]),
            tt_sitestats_report_view=sum(EVENT_COUNT[EVENT_ID=="sitestats.report.view"]),
            tt_asn_delete_assignment=sum(EVENT_COUNT[EVENT_ID=="asn.delete.assignment"]),
            tt_webcontent_revise=sum(EVENT_COUNT[EVENT_ID=="webcontent.revise"]),
            tt_asn_new_assignment=sum(EVENT_COUNT[EVENT_ID=="asn.new.assignment"]),
            tt_sam_total_score_update=sum(EVENT_COUNT[EVENT_ID=="sam.total.score.update"]),
            tt_sam_pubassessment_confirm_edit=sum(EVENT_COUNT[EVENT_ID=="sam.pubassessment.confirm_edit"]),.groups = "drop") %>% 
  mutate(across(where(is.numeric), ~ ifelse(is.na(.), 0, .))) %>% 
  mutate(across(where(is.double), as.integer))

sst_events %>% 
  sdf_coalesce(1) %>% 
  spark_write_parquet(.,"./TFM_PROD/preprocess_data/sst_events_summarize",mode="overwrite")


sst_resources <- 
  spark_read_parquet(sc,path = "./Datos_Sakai_Parquet/sst_resources") %>% 
  mutate(split_site=split(SITE_ID,"-"),) %>% 
  sdf_separate_column(column = "split_site", into = c("SID","IDMASTER","IDASIGNATURA","IDPARALELO")) %>% 
  mutate(SITE_UID = paste0(SID,"-",IDMASTER,"-",IDPARALELO),
         IDMASTER = as.integer(IDMASTER),
         RESOURCE_REF= as.character(encode(RESOURCE_REF,"windows-1252")),
         PERIODO=as.integer(date_format(RESOURCE_DATE,"yyyyMM")),
         nweek=date_format(RESOURCE_DATE,'W'),
         dweek=date_format(RESOURCE_DATE,'E'),
         fds=if_else(dweek%in%c("Sun","Sat"),1,0),
         tipo=case_when(rlike(toupper(RESOURCE_REF),"FORO") ~ "Foros",
                        rlike(toupper(RESOURCE_REF),"DOCUMENTACIÓN") ~ "Documentacion",
                        rlike(toupper(RESOURCE_REF),"MENSAJES PRIVADOS") ~ "Mensajes",
                        rlike(toupper(RESOURCE_REF),"ASSIGNMENTS|ACTIVIDADES|TAREAS") ~ "Actividades",
                        contains(toupper(RESOURCE_REF),"PSPDF") ~ "Programacion",
                        T ~ "Otros")) %>% 
  left_join(sakai_site_user %>% distinct(),.,by=c("SITE_UID","SITE_ID","USER_ID")) %>% 
  mutate(PERIODO_WEEK = as.integer((datediff(RESOURCE_DATE,FECHA_INICIO)/7) +1)) %>% filter(!is.na(RESOURCE_DATE)) %>% 
  group_by(SITE_UID,PERIODO_WEEK,USER_ID) %>% 
  summarise(tres_up_foros=sum(RESOURCE_COUNT[tipo=="Foros" & RESOURCE_ACTION=="new"]),
            tres_down_foros=sum(RESOURCE_COUNT[tipo=="Foros" & RESOURCE_ACTION=="read"]),
            tres_up_doc=sum(RESOURCE_COUNT[tipo=="Documentacion" & RESOURCE_ACTION=="new"]),
            tres_down_doc=sum(RESOURCE_COUNT[tipo=="Documentacion" & RESOURCE_ACTION=="read"]),
            tres_up_msg=sum(RESOURCE_COUNT[tipo=="Mensajes" & RESOURCE_ACTION=="new"]),
            tres_down_msg=sum(RESOURCE_COUNT[tipo=="Mensajes" & RESOURCE_ACTION=="read"]),
            tres_up_tareas=sum(RESOURCE_COUNT[tipo=="Actividades" & RESOURCE_ACTION=="new"]),
            tres_down_tareas=sum(RESOURCE_COUNT[tipo=="Actividades" & RESOURCE_ACTION=="read"]),
            tres_up_prog=sum(RESOURCE_COUNT[tipo=="Programacion" & RESOURCE_ACTION=="new"]),
            tres_down_prog=sum(RESOURCE_COUNT[tipo=="Programacion" & RESOURCE_ACTION=="read"]),
            tres_up_otros=sum(RESOURCE_COUNT[tipo=="Otros" & RESOURCE_ACTION=="new"]),
            tres_down_otros=sum(RESOURCE_COUNT[tipo=="Otros" & RESOURCE_ACTION=="read"]),.groups = "drop") %>% 
  mutate(across(where(is.numeric), ~ ifelse(is.na(.), 0, .))) %>% 
  mutate(across(where(is.double), as.integer))

#sst_resources %>% sdf_collect() %>% write_excel_csv(.,"./TFM_PROD/preprocess_data/sst_resources_clean.csv")

sst_resources %>% 
  sdf_coalesce(1) %>% 
  spark_write_parquet(.,"./TFM_PROD/preprocess_data/sst_resources_summarize",mode="overwrite")



sst_userstats <- 
  spark_read_parquet(sc,path = "./Datos_Sakai_Parquet/sst_userstats") %>% 
  mutate(PERIODO=as.integer(date_format(LOGIN_DATE,"yyyyMM")),
         nweek=date_format(LOGIN_DATE,'W'),
         dweek=date_format(LOGIN_DATE,'E'),
         fds=if_else(dweek%in%c("Sun","Sat"),1,0)) %>% 
  left_join(sakai_site_user %>% select(SITE_UID,FECHA_INICIO,USER_ID) %>% distinct(),.,by=c("USER_ID")) %>% 
  mutate(PERIODO_WEEK = as.integer((datediff(LOGIN_DATE,FECHA_INICIO)/7) +1)) %>% filter(!is.na(LOGIN_DATE) & PERIODO_WEEK>=1) %>% 
  group_by(SITE_UID,PERIODO_WEEK,USER_ID) %>% 
  summarise(tt_login=sum(LOGIN_COUNT))

sst_userstats %>% 
  sdf_coalesce(1) %>% 
  spark_write_parquet(.,"./TFM_PROD/preprocess_data/sst_userstats_summarize",mode="overwrite")

spark_disconnect_all()











