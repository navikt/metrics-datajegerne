import logging

from stage_etterlevelsebesvarelse import run_etl_etterlevelsebesvarelse
from stage_krav import run_etl_tema
from stage_etterlevelsedokument import run_etl_dokumenter
from stage_meldinger import run_etl_spoersmaal_og_svar
from stage_relasjoner import run_etl_relasjoner
from stage_tildeltognotater import run_etl_tildelt_og_notater
from stage_behandlingskatalogen import run_etl_behandlinger, run_etl_information_types, run_etl_legal_bases, run_etl_systems, run_etl_dataprocessors

# Datasets
from datasets import run_etl_datasett_varslinger, run_etl_datasett_beskrivelser, run_etl_datasett_prioritertlist, run_etl_datasett_gjenbruk, run_etl_sist_oppdatert




if __name__ == "__main__":
    # Setter opp logging
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.ERROR)
    logging.info("Jobben starter!")

    # Flytting og transformering av data fra kilde
    run_etl_behandlinger() # Behandlinger
    run_etl_information_types() # Knytning mellom policies og information types
    run_etl_legal_bases() # Behandlingsgrunnlag med beskrivelser
    run_etl_systems() # systemer brukt i behandling
    run_etl_dataprocessors() # databehandlere
    logging.info("Behandlingskatalogen kjørt")

    run_etl_etterlevelsebesvarelse() # Besvarelser fra etterlevere
    logging.info("Besvarelser kjørt!")
    run_etl_dokumenter() # Dokumentegenskaper
    logging.info("Dokumenter kjørt!")
    run_etl_tema() # krav fordelt på tema
    logging.info("Krav og tema kjørt!")
    run_etl_spoersmaal_og_svar() # spørsmål og svar til kraveier
    logging.info("Spørsmål og svar kjørt!")
    run_etl_relasjoner() # koblinger mellom dokumenter
    logging.info("Relasjoner kjørt!")
    run_etl_tildelt_og_notater() # Viser hvilke krav som er tildelt til hvem og hvor notater er skrevet
    logging.info("Tildelt og notater kjørt!")


    # Videre transformering i python siden vi ikke orker å gjøre alt i SQL: Dette blir egne tabeller som blir datasett på Markedsplassen
    run_etl_datasett_varslinger()
    logging.info("Varslinger kjørt!")
    run_etl_datasett_beskrivelser()
    logging.info("Beskrivelser kjørt!")
    run_etl_datasett_prioritertlist()
    logging.info("Prioritert kjørt kjørt!")
    run_etl_datasett_gjenbruk()
    logging.info("Gjenbruk kjørt!")
    run_etl_sist_oppdatert()
    logging.info("Sist oppdatert kjørt!")
