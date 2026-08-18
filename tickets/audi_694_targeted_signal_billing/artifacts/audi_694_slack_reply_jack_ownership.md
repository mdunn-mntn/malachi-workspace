# Draft reply to Jack's ownership breakdown (2026-08-18)

That breakdown helps a lot, and the List 1 / 2 / 3 naming is worth keeping.

List 1 is mine, and your note that DS63 doesn't need it is the piece I was missing. That makes AUDI-694
an MNTN ID ticket, not a DS63 one. Sean's already got the table, so my job is making sure it actually
feeds the crediting run when MNTN ID lands.

On crediting logic, no, I'm not implementing it and don't want to duplicate Maya. What I do have is the
DDP analysis from AUDI-1089 and 1111, so I'd offer to own the recommendation for how List 1 gets
credited and let Maya implement.

One flag on that. You wrote List 1 will continue to get fractional credit at a fixed CPM. That's the
thing our analysis pushed back on. We're paying metered vendors for signal our own guid and augmentor
logs already cover, about $769k a year, and Sherwin reproduced it independently on BAE-4923. So I'd
rather we decide List 1 crediting deliberately than inherit it.

Happy to circle up. The one thing that seems time sensitive is that Maya's already using the new logic
for the August payout.
