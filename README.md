# utl-join-two-tables-based-on-id-date-and-nearest-time-r-and-sql-sas-r-python-muti-language
Join two tables based on id date and nearest time r and sql sas r python muti language
    %let pgm=utl-join-two-tables-based-on-id-date-and-nearest-time-r-and-sql-sas-r-python-muti-language;

    Join two tables based on id date and nearest time r and sql sas r python muti language

    github
    https://tinyurl.com/2m7m6wuh
    https://github.com/rogerjdeangelis/utl-join-two-tables-based-on-id-date-and-nearest-time-r-and-sql-sas-r-python-muti-language

    %stop_submission;

    related.to
    https://tinyurl.com/2ht4uu4y
    https://stackoverflow.com/questions/79287593/how-to-join-two-datasets-based-on-the-id-date-and-approximate

    I think this solution is more general than the posted solutions;
    I do not use the statement that the second time is 10 minutes greater than the first.

      SOLUTIONS

        1 sas sql
        2 r sql
        3 python sql

    /*               _     _
     _ __  _ __ ___ | |__ | | ___ _ __ ___
    | `_ \| `__/ _ \| `_ \| |/ _ \ `_ ` _ \
    | |_) | | | (_) | |_) | |  __/ | | | | |
    | .__/|_|  \___/|_.__/|_|\___|_| |_| |_|
    |_|
    */

    /*****************************************************************************************************************************************/
    /*                                |                                        |                                                             */
    /*                 INPUT          |             PROCESS                    |                        OUTPUT                               */
    /*                 =====          |             =======                    |                        ======                               */
    /*                                |                                        |                                                             */
    /*   SD1.DF1 total obs=4          |  1 SAS SQL                             |                                                             */
    /*                                |  ========                              | SD1.WANT                                                    */
    /*    ID      W      DATETIME     |  self explanatory                      |                                                             */
    /*                                |                                        | DF                                         MIN              */
    /*     1    0.25    1922551321    |                                        | ID        DF1_DATETIME        DF2_DATETIME  DIF     X1  W   */
    /*     1    0.35    1922555181    |  select                                |                                                             */
    /*     2    0.44    1922551928    |    df1.id     as df1_id                |  1 2020-12-02T18:02:01 2020-12-02T18:08:01 6.00  0.25 1.3   */
    /*     3    0.98    1922608983    |   ,put(df1.datetime,E8601DT.)          |  2 2020-12-02T18:12:08 2020-12-02T18:21:11 9.05  0.44 4.2   */
    /*                                |      as df1_datetime                   |  3 2020-12-03T10:03:03 2020-12-03T10:09:22 6.31  0.98 7.1   */
    /*                                |   ,put(df2.datetime,E8601DT.)          |                                                             */
    /*   SD1.DF2 total obs=3          |       as df2_datetime                  |                                                             */
    /*                                |   ,abs(df1.datetime-df2.datetime)      |                                                             */
    /*    ID     X1     DATETIME      |       /60 as mindif                    |                                                             */
    /*                                |   ,df1.w                               |                                                             */
    /*     1    1.3    1922551681     |   ,df2.x1                              |                                                             */
    /*     2    4.2    1922552471     |  from                                  |                                                             */
    /*     3    7.1    1922609362     |    sd1.df1 full outer join sd1.df2     |                                                             */
    /*                                |  on                                    |                                                             */
    /*   options validvarname=upcase; |   df1.id = df2.id                      |                                                             */
    /*   libname sd1 "d:/sd1";        |  group                                 |                                                             */
    /*   data sd1.df1;                |   by df1.id                            |                                                             */
    /*     retain id w;               |  having                                |                                                             */
    /*   input  DATETIME ID  W;       |  min(abs(df1.datetime-df2.datetime))=  |                                                             */
    /*   cards4;                      |    abs(df1.datetime-df2.datetime)      |                                                             */
    /*   1922551321 1 0.25            |                                        |                                                             */
    /*   1922555181 1 0.35            |                                        |                                                             */
    /*   1922551928 2 0.44            | 2 R AND PYTHON (EXACTLY THE SAME)      |                                                             */
    /*   1922608983 3 0.98            | =================================      |                                                             */
    /*   ;;;;                         |                                        |                                                             */
    /*   run;quit;                    |  select                                |                                                             */
    /*                                |    df1.id as df1_id                    |                                                             */
    /*   data sd1.df2;                |   ,strftime("%Y-%m-%dT%H:%M:%S"        |                                                             */
    /*     retain id x1;              |         ,df1.datetime-3653*86400       |                                                             */
    /*   input  DATETIME ID  x1;      |         ,"unixepoch") as df1_datetimec |                                                             */
    /*   cards4;                      |   ,strftime("%Y-%m-%dT%H:%M:%S"        |                                                             */
    /*   1922551681 1 1.3             |         ,df2.datetime-3653*86400       |                                                             */
    /*   1922552471 2 4.2             |         ,"unixepoch") as df2_datetimec |                                                             */
    /*   1922609362 3 7.1             |   ,abs(df1.datetime-df2.datetime)/60   |                                                             */
    /*   ;;;;                         |     as mindif                          |                                                             */
    /*   run;quit;                    |   ,df1.w                               |                                                             */
    /*                                |   ,df2.x1                              |                                                             */
    /*                                |  from                                  |                                                             */
    /*                                |    df1 full outer join df2             |                                                             */
    /*                                |  on                                    |                                                             */
    /*                                |   df1.id = df2.id                      |                                                             */
    /*                                |  group                                 |                                                             */
    /*                                |   by df1.id                            |                                                             */
    /*                                |  having                                |                                                             */
    /*                                |    min(abs(df1.datetime-df2.datetime)) |                                                             */
    /*                                |       = abs(df1.datetime-df2.datetime) |                                                             */
    /*                                |                                        |                                                             */
    /*****************************************************************************************************************************************/

    /*                   _
    (_)_ __  _ __  _   _| |_
    | | `_ \| `_ \| | | | __|
    | | | | | |_) | |_| | |_
    |_|_| |_| .__/ \__,_|\__|
            |_|
    */
    options validvarname=upcase;
    libname sd1 "d:/sd1";
    data sd1.df1;
      retain id w;
    input  DATETIME ID  W;
    cards4;
    1922551321 1 0.25
    1922555181 1 0.35
    1922551928 2 0.44
    1922608983 3 0.98
    ;;;;
    run;quit;

    data sd1.df2;
      retain id x1;
    input  DATETIME ID  x1;
    cards4;
    1922551681 1 1.3
    1922552471 2 4.2
    1922609362 3 7.1
    ;;;;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*  SD1.DF1 total obs=4                                                                                                   */
    /*                                                                                                                        */
    /*   ID      W      DATETIME                                                                                              */
    /*                                                                                                                        */
    /*    1    0.25    1922551321                                                                                             */
    /*    1    0.35    1922555181                                                                                             */
    /*    2    0.44    1922551928                                                                                             */
    /*    3    0.98    1922608983                                                                                             */
    /*                                                                                                                        */
    /*                                                                                                                        */
    /*  SD1.DF2 total obs=3                                                                                                   */
    /*                                                                                                                        */
    /*   ID     X1     DATETIME                                                                                               */
    /*                                                                                                                        */
    /*    1    1.3    1922551681                                                                                              */
    /*    2    4.2    1922552471                                                                                              */
    /*    3    7.1    1922609362                                                                                              */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*                    _
    / |  _ __   ___  __ _| |
    | | | `__| / __|/ _` | |
    | | | |    \__ \ (_| | |
    |_| |_|    |___/\__, |_|
                       |_|
    */

    %utl_rbeginx;
    parmcards4;
    library(haven)
    library(sqldf)
    source("c:/oto/fn_tosas9x.R")
    df1<-read_sas("d:/sd1/df1.sas7bdat")
    df2<-read_sas("d:/sd1/df2.sas7bdat")
    print(df1)
    print(df2)
    want<-sqldf('
      select
        df1.id as df1_id
       ,strftime("%Y-%m-%dT%H:%M:%S"
             ,df1.datetime-3653*86400
             ,"unixepoch") as df1_datetimec
       ,strftime("%Y-%m-%dT%H:%M:%S"
             ,df2.datetime-3653*86400
             ,"unixepoch") as df2_datetimec
       ,abs(df1.datetime-df2.datetime)/60 as mindif
       ,df1.w
       ,df2.x1
      from
        df1 full outer join df2
      on
       df1.id = df2.id
      group
       by df1.id
      having
        min(abs(df1.datetime-df2.datetime))
           = abs(df1.datetime-df2.datetime)
      ')
    want
    r_datetime <- as.POSIXct(want$df1_datetime
      ,format = "%Y-%m-%dT%H:%M:%OS", tz = "UTC")
    r_datetime
    fn_tosas9x(
          inp    = want
         ,outlib ="d:/sd1/"
         ,outdsn ="want"
         )
    ;;;;
    %utl_rendx;

    proc print data=sd1.want;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*  R                                                                                                                     */
    /*                                                                                                                        */
    /*    DF1_ID       DF1_DATETIMEC       DF2_DATETIMEC   MINDIF    W  X1                                                    */
    /*                                                                                                                        */
    /*  1      1 2020-12-02T18:02:01 2020-12-02T18:08:01 6.000000 0.25 1.3                                                    */
    /*  2      2 2020-12-02T18:12:08 2020-12-02T18:21:11 9.050000 0.44 4.2                                                    */
    /*  3      3 2020-12-03T10:03:03 2020-12-03T10:09:22 6.316667 0.98 7.1                                                    */
    /*                                                                                                                        */
    /*  SAS                                                                                                                   */
    /*                                                                                                                        */
    /*  ROWNAMES    DF1_ID       DF1_DATETIMEC          DF2_DATETIMEC        MINDIF      W      X1                            */
    /*                                                                                                                        */
    /*      1          1      2020-12-02T18:02:01    2020-12-02T18:08:01    6.00000    0.25    1.3                            */
    /*      2          2      2020-12-02T18:12:08    2020-12-02T18:21:11    9.05000    0.44    4.2                            */
    /*      3          3      2020-12-03T10:03:03    2020-12-03T10:09:22    6.31667    0.98    7.1                            */
    /*                                                                                                                        */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*____               _   _
    |___ /   _ __  _   _| |_| |__   ___  _ __
      |_ \  | `_ \| | | | __| `_ \ / _ \| `_ \
     ___) | | |_) | |_| | |_| | | | (_) | | | |
    |____/  | .__/ \__, |\__|_| |_|\___/|_| |_|
            |_|    |___/
    */


    proc datasets lib=sd1 nolist nodetails;
     delete pywant;
    run;quit;

    %utl_pybeginx;
    parmcards4;
    exec(open('c:/oto/fn_python.py').read());
    df1,meta = ps.read_sas7bdat('d:/sd1/df1.sas7bdat');
    df2,meta = ps.read_sas7bdat('d:/sd1/df2.sas7bdat');
    want=pdsql('''                                    \
      select                                          \
        df1.id     as df1_id                          \
       ,strftime("%Y-%m-%dT%H:%M:%S"                  \
             ,df1.datetime-3653*86400                 \
             ,"unixepoch") as df1_datetimec           \
       ,strftime("%Y-%m-%dT%H:%M:%S"                  \
             ,df2.datetime-3653*86400                 \
             ,"unixepoch") as df2_datetimec           \
       ,abs(df1.datetime-df2.datetime)/60 as mindif   \
       ,df2.x1                                        \
      from                                            \
        df1 full outer join df2                       \
      on                                              \
       df1.id = df2.id                                \
      group                                           \
       by df1.id                                      \
      having                                          \
        min(abs(df1.datetime-df2.datetime))           \
           = abs(df1.datetime-df2.datetime)           \
       ''');
    print(want);
    fn_tosas9x(want,outlib='d:/sd1/',outdsn='pywant',timeest=3);
    ;;;;
    %utl_pyendx;

    proc print data=sd1.pywant;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*                                                                                                                        */
    /* PYTHON                                                                                                                 */
    /*                                                                                                                        */
    /*    df1_id        df1_datetimec        df2_datetimec    mindif   X1                                                     */
    /*                                                                                                                        */
    /* 0     1.0  2020-12-02T18:02:01  2020-12-02T18:08:01  6.000000  1.3                                                     */
    /* 1     2.0  2020-12-02T18:12:08  2020-12-02T18:21:11  9.050000  4.2                                                     */
    /* 2     3.0  2020-12-03T10:03:03  2020-12-03T10:09:22  6.316667  7.1                                                     */
    /*                                                                                                                        */
    /* SAS                                                                                                                    */
    /*                                                                                                                        */
    /* DF1_ID       DF1_DATETIMEC          DF2_DATETIMEC        MINDIF     X1                                                 */
    /*                                                                                                                        */
    /*    1      2020-12-02T18:02:01    2020-12-02T18:08:01    6.00000    1.3                                                 */
    /*    2      2020-12-02T18:12:08    2020-12-02T18:21:11    9.05000    4.2                                                 */
    /*    3      2020-12-03T10:03:03    2020-12-03T10:09:22    6.31667    7.1                                                 */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*              _
      ___ _ __   __| |
     / _ \ `_ \ / _` |
    |  __/ | | | (_| |
     \___|_| |_|\__,_|

    */
