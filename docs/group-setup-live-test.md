# Group setup live checks

These checks remain open. Offline tests use fake Telegram transport. They do
not establish Telegram client behavior. Use a second Mac, a fresh test bot,
and a fresh test owner account. Never use the production bot or this Mac.
Record the client name and version.

1. Run `setup.command`. Confirm the terminal text and hidden token input.
2. Open the group link. Check whether the picker offers **New group** and
   whether Manage topics, Delete messages, and Pin messages are preselected.
3. Check where `/start` lands, whether `from` is the owner, and whether the
   `my_chat_member` performer matches that sender.
4. Start with a basic group. Turn Topics on. Check migration, **Check again**,
   and whether the bot keeps administrator status with Manage topics.
5. Continue in **Torii**, connect Claude by pasting the code in the topic, and name
   the first project. Confirm the held request reaches its new topic.
6. Untick Manage topics and test **Fix permissions**. Add the bot to a second
   group and confirm it leaves. Test an anonymous pairing sender, owner leave
   and rejoin, and bot removal and re-addition.
7. Repeat with an existing forum supergroup. Check deleted project topics and
   recreation of the Torii topic.
8. On the separate private test install's Mac, check refusal and explicit
   `pair --replace` conversion. Preserve projects, accounts, and native history.
9. After automatic token pickup, paste the token and press Return during the
   Telegram check. Confirm the token stays hidden and Claude login does not
   receive it. Confirm no paste is needed after pickup.
10. Run setup over SSH on the spare Mac. Confirm setup connects Claude from
    `claude auth status` without any Torii Keychain prompt. Then test a signed-out
    profile. Confirm setup and the Telegram card say Claude is not connected,
    and **Connect Claude** stays available after a service restart and account
    monitor refresh.

Report observed replies, tap counts, and unresolved checks. Do not report
credentials, pairing codes, or native transcripts.
