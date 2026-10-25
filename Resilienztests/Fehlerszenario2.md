Fehlerszenario 2: faultyProducer sendet alle x Sekunden eine Nachricht mit falschem Typ


1. Producer sendet jede n-te Nachricht defekt (mit falschem Typ)
2. Sie wird als ungültig markeirt und landet in DLQ
3. NACK
4. Nein, sie wird direkt in DLQ geschoben, da eine ungültige Nachricht nicht gültig werden kann
5. Über validate() im analyse_consumer und ein try-except-Block, der die Exception wirft
6. Bei einer ungültigen Nachricht
7. Ja

