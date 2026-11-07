from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from cdispatch.common import atomic,read
from cdispatch.watchdog import notify


class WatchdogAlertTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)
        self.token=self.root/'token';self.token.write_text('test-token');self.token.chmod(0o600)
        self.settings=self.root/'settings.json'
        atomic(self.settings,dict(notifications=True,chat_id=12,allowed_user_ids=[12],token_file=str(self.token),state_file=str(self.root/'bot-state.json'),proxy=dict(port=10808)))
        self.settings.chmod(0o600)
        atomic(self.root/'telegram-status.json',dict(time=1000,error=None))
        self.config=dict(telegram_settings=str(self.settings))

    def test_service_death_warning_is_once_and_recovery_is_once(self):
        with patch('cdispatch.telegram.TelegramAPI') as api:
            notify(self.root,self.config,dict(heartbeat=0),1000)
            notify(self.root,self.config,dict(heartbeat=0),1001)
            self.assertEqual(api.return_value.call.call_count,1)
            self.assertEqual(api.call_args.args[1],'socks5h://127.0.0.1:10808')
            notify(self.root,self.config,dict(heartbeat=1002),1002)
            notify(self.root,self.config,dict(heartbeat=1003),1003)
            self.assertEqual(api.return_value.call.call_count,2)

    def test_delivery_failure_preserves_unsent_warning_for_next_check(self):
        with patch('cdispatch.telegram.TelegramAPI') as api:
            api.return_value.call.side_effect=OSError('transport unavailable')
            notify(self.root,self.config,dict(heartbeat=0),1000)
            self.assertFalse(read(self.root/'watchdog-alerts.json').get('issue'))
            api.return_value.call.side_effect=None
            notify(self.root,self.config,dict(heartbeat=0),1001)
            self.assertEqual(api.return_value.call.call_count,2)

    def test_user_can_disable_all_automatic_delivery(self):
        atomic(self.root/'bot-state.json',dict(alerts=False))
        with patch('cdispatch.telegram.TelegramAPI') as api:
            notify(self.root,self.config,dict(heartbeat=0),1000)
            api.assert_not_called()

    def test_healthy_service_and_bot_do_not_send_messages(self):
        with patch('cdispatch.telegram.TelegramAPI') as api:
            notify(self.root,self.config,dict(heartbeat=1000),1000)
            api.assert_not_called()


if __name__=='__main__':unittest.main()
