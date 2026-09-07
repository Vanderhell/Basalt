Implementuj BasaltCore Domain Atomicity.

Urob:

- atomic `jobcore_enqueue()`:
  - payload + execution READY + submitted stats v jednom commit-e
- atomic finalization:
  - DONE/FAILED/DEAD/CANCELLED + finished_at + ledger + príslušné stats v jednom commit-e
- atomic retry:
  - state + attempt + eligible_at + retried_total v jednom commit-e
- odstráň všetky partial-state scenáre medzi týmito operáciami

Doplň cielené testy pre:
- crash/failure medzi jednotlivými krokmi enqueue/finalization/retry
- conflict/race na execution revision
- stats a ledger konzistenciu
- reopen/recovery po durable commit
- stale lease pri finalization/retry

Implementuj, testuj, oprav zlyhania a pokračuj cez všetky body. Skonči až keď relevantné BasaltDB/BasaltCore testy prejdú.