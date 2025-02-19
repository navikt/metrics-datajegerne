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
    stage_relasjoner }|--|{ stage_dokument: etterlevelseDokumentasjonIdFra
    stage_relasjoner }|--|{ stage_dokument: etterlevelseDokumentasjonIdTil

    stage_pvk_dokument ||--|| stage_besvarelse: etterlevelseDokumentasjonId
    stage_pvk_dokument ||--|{ stage_risikoscenario: pvkDokumentId
    stage_pvk_dokument ||--|{ stage_tiltak: pvkDokumentId

    stage_behandlinger }|--|| stage_dokument: behandlingId
    stage_policy }|--|| stage_informationType: informationTypeId
    stage_policy  ||--|| stage_bridge_policy_behandling: policyId
    stage_behandlinger ||--|{  stage_bridge_policy_behandling: behandlingId
    stage_behandlinger ||--|{  stage_dataprocessors: behandlingId
    stage_behandlinger ||--|{  stage_systems: behandlingId
    stage_behandlinger ||--|{  stage_behandlingsgrunnlag: behandlingId

    

    stage_besvarelse {
        string etterlevelseDokumentasjonId
        int kravNummer
        int suksesskriterieId
        string aktivRad
        string se_BigQuery
    }
    stage_dokument {
        string etterlevelseDokumentasjonId
        array behandlingId
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

    stage_relasjoner {
        string etterlevelseDokumentasjonIdFra
        string etterlevelseDokumentasjonIdTil
        string relationType
    }




    stage_pvk_dokument {
        string etterlevelseDokumentasjonId
        string pvkDokumentId
        string se_BigQuery
    }

    stage_risikoscenario {
        string risikoscenarioId
        string pvkDokumentId
        string se_BigQuery
    }

    stage_tiltak {
        string tiltakId
        string pvkDokumentId
        string se_BigQuery
    }





    stage_behandlinger {
        string behandlingId
        string se_BigQuery
    }
    stage_dataprocessors {
        string dataprocessorI
        string dbehandlingId
        string se_BigQuery
    }
    stage_systems {
        string code
        string behandlingId
        string se_BigQuery
    }
    stage_behandlingsgrunnlag {
        string behandlingId
        string se_BigQuery
    }
    stage_policy {
        string policyId
        string informationTypeId
        string se_BigQuery
    }
    stage_bridge_policy_behandling {
        string behandlingId
        string policyId
        string se_BigQuery
    }
    stage_informationType {
        string informationTypeId
        string se_BigQuery
    }
```