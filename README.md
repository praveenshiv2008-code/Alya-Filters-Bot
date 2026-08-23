# Alya Filter Bot

A powerful Telegram filter bot with anti-link protection, auto-welcome, and media filter support.

## Features

- ✅ Auto-Reply Filters (Text & Media)
- ✅ Anti-Link Protection with Temp Ban
- ✅ Auto Welcome Messages
- ✅ Admin Management
- ✅ User Management (Ban/Unban)
- ✅ Broadcast Messages
- ✅ Filter Groups & Categories
- ✅ Export/Import Filters
- ✅ Statistics & Logs
- ✅ Backup & Restore
- ✅ Anime Style UI

## Commands

### User Commands
- `/start` - Start the bot
- `/help` - Show help
- `/listfilters` - List all filters
- `/filterstats` - Filter statistics
- `/ping` - Check bot status

### Admin Commands
- `/addfilter` - Add filter
- `/editfilter` - Edit filter
- `/delfilter` - Delete filter
- `/addreply` - Add reply to filter
- `/delreply` - Delete reply from filter
- `/filtergroup` - Set filter group
- `/filterinfo` - Filter details
- `/listfilters` - List all filters
- `/filterstats` - Filter stats
- `/exportfilters` - Export filters
- `/importfilters` - Import filters
- `/ban` - Ban user
- `/unban` - Unban user
- `/userinfo` - User details
- `/users` - User statistics
- `/antilink` - Anti-link settings
- `/welcome` - Welcome settings
- `/broadcast` - Broadcast message
- `/backup` - Create backup
- `/restore` - Restore backup
- `/logs` - View logs
- `/stats` - Bot statistics
- `/maintenance` - Maintenance mode

### Owner Commands
- `/addadmin` - Add admin
- `/deladmin` - Remove admin
- `/admins` - List admins

## Deployment

### Render.com

1. Create a new Web Service on Render
2. Connect your GitHub repository
3. Set environment variables:
   - `API_ID`
   - `API_HASH`
   - `BOT_TOKEN`
   - `MONGO_URI`
   - `OWNER_ID`
   - `BACKUP_CHANNEL_ID`
4. Deploy!

### Environment Variables

| Variable | Description |
|----------|-------------|
| `API_ID` | Telegram API ID |
| `API_HASH` | Telegram API Hash |
| `BOT_TOKEN` | Bot Token from @BotFather |
| `MONGO_URI` | MongoDB Connection String |
| `OWNER_ID` | Your Telegram User ID |
| `BACKUP_CHANNEL_ID` | Channel ID for backups |

## Developer

👑 **Developed by [Prime Core](https://t.me/PrimeCoreHQ)**

## License

MIT License