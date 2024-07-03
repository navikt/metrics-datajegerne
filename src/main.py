from tema import run_etl_tema
from prioriterte_krav import run_etl_prioriterte_krav
from sist_oppdatert import run_etl_sist_oppdatert
from alerts import run_etl_alerts
from risikoeier import run_etl_risikoeier
from beskrivelser import run_etl_beskrivelser
from suksesskriterier import run_etl_suksesskriterier
from websak import run_etl_websak
from spoersmaalogsvar import run_etl_spoersmaal_og_svar



if __name__ == "__main__":
    run_etl_tema() # kobling mellom krav, tema og regelverk
    run_etl_prioriterte_krav() # prioriterte_krav
    run_etl_sist_oppdatert() # sist oppdatert på dokument-nivå
    run_etl_alerts() # alerts
    run_etl_risikoeier() # risikoeiere
    run_etl_beskrivelser() # beskrivelser
    run_etl_suksesskriterier() # besvarelser
    run_etl_websak() # arkivering
    run_etl_spoersmaal_og_svar() # spørsmål og svar

    print("Jobben kjørt!")

