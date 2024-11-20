from ny_etterlevelsebesvarelse import run_etl_etterlevelsebesvarelse
from ny_krav import run_etl_tema
from ny_etterlevelsedokument import run_etl_dokumenter
from ny_meldinger import run_etl_spoersmaal_og_svar



if __name__ == "__main__":
    run_etl_etterlevelsebesvarelse() # Besvarelser fra etterlevere
    run_etl_dokumenter() # Dokumentegenskaper
    run_etl_tema() # krav fordelt på tema
    run_etl_spoersmaal_og_svar() # spørsmål og svar til kraveier
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

    print("Jobben kjørt!")

