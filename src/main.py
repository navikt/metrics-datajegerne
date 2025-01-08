import logging

from stage_etterlevelsebesvarelse import run_etl_etterlevelsebesvarelse
from stage_krav import run_etl_tema
from stage_etterlevelsedokument import run_etl_dokumenter
from stage_meldinger import run_etl_spoersmaal_og_svar
from stage_relasjoner import run_etl_relasjoner
from stage_tildeltognotater import run_etl_tildelt_og_notater

# Datasets
from datasets import run_etl_datasett_varslinger, run_etl_datasett_beskrivelser, run_etl_datasett_prioritertlist, run_etl_datasett_gjenbruk, run_etl_sist_oppdatert




if __name__ == "__main__":
    # Setter opp logging
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)
    logging.info("Jobben starter!")

    # Flytting og transformering av data fra kilde
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


    """
    run_etl_tema() # kobling mellom krav, tema og regelverk
    run_etl_prioriterte_krav() # prioriterte_krav
    run_etl_sist_oppdatert() # sist oppdatert på dokument-nivå
    run_etl_alerts() # alerts
    run_etl_risikoeier() # risikoeiere
    run_etl_beskrivelser() # beskrivelser
    run_etl_suksesskriterier() # besvarelser
    run_etl_websak() # arkivering
    run_etl_spoersmaal_og_svar() # spørsmål og svar
    run_etl_tildelt_og_notater() # tildelt til krav og notatfunksjon
    run_etl_duplicates() # for å sjekke duplikater i etterlevelse
    run_etl_mordokumenter() # for å følge med på hvor mange som har mordokumenter
    """

