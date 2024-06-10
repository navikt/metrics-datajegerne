from prioriterte_krav import run_etl_prioriterte_krav
from sist_oppdatert import run_etl_sist_oppdatert
from alerts import run_etl_alerts
from risikoeier import run_etl_risikoeier
from beskrivelser import run_etl_beskrivelser



if __name__ == "__main__":
    run_etl_prioriterte_krav() # prioriterte_krav
    run_etl_sist_oppdatert() # sist oppdatert på dokument-nivå
    run_etl_alerts() # alerts
    run_etl_risikoeier() # risikoeiere
    run_etl_beskrivelser() # beskrivelser

