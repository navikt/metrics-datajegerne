# Metrics Datajegerne

Henter og transformerer data fra Støtte til Etterlevelse.

Dataene brukes til å forstå hvordan folk bruker verktøyet.

Data fra Postgres i Etterlevelse transformeres og lastes til "staging" med scheduled query. 
Jobbene henter data fra følgende tabeller:
- Generic storage
- codelist
- audit version

Modellen ser sånn ut:

```mermaid
erDiagram
    stage_besvarelse }|--|{ stage_dokument : etterlevelseDokumentasjonId
    stage_besvarelse }|--|| stage_krav : kravNummer
    stage_meldinger }|--|| stage_krav: kravNummer

    stage_besvarelse {
        string etterlevelseDokumentasjonId
        int kravNummer
        int suksesskriterieId
        string aktivRad
        string se_BigQuery
    }
    stage_dokument {
        string etterlevelseDokumentasjonId
        string aktivRad
        string se_BigQuery
    }
    stage_krav {
        int kravNummer
        string tema
        string regelverk
        string underavdeling

    }    
    stage_meldinger {
        int kravNummer
        string se_BigQuery
    }
```