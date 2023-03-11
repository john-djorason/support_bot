"""
Telegram bot implementation using Telethon
"""

import os
import logging
import configparser
import json
import pyodbc
from pandas import DataFrame
from telethon import TelegramClient, events, functions, Button


class BotSettings:
    """A class to represent a Telegram-bot settings."""

    _parser = None

    def __init__(self, path='config.ini'):
        parser = configparser.ConfigParser()
        parser.read(path, 'utf-8')
        self._parser = parser

    def token(self):
        return self._read_setting('CREDENTIALS', 'TOKEN')

    def api_name(self):
        return self._read_setting('CREDENTIALS', 'NAME')

    def api_id(self):
        return int(self._read_setting('CREDENTIALS', 'ID'))

    def api_hash(self):
        return self._read_setting('CREDENTIALS', 'HASH')

    def path_logs(self):
        return self._read_setting('PATHS', 'LOGS')

    def path_auth(self):
        return self._read_setting('PATHS', 'AUTH')

    def path_users(self):
        return self._read_setting('PATHS', 'USERS')

    def path_doc(self):
        return self._read_setting('PATHS', 'DOC')

    def path_media(self, user):
        return self._read_setting('PATHS', 'MEDIA') + str(user) + '\\'

    def default_manager(self):
        return int(self._read_setting('MANAGERS', 'DEFAULT'))

    def documents_manager(self):
        return int(self._read_setting('MANAGERS', 'DOCUMENTS'))

    def admin_manager(self):
        return int(self._read_setting('MANAGERS', 'ADMIN'))

    def _read_setting(self, section, name):
        return self._parser[section][name]


class Bot:
    """
    A class to represent a Telegram-bot with all of its functionality.

    Methods
    -------
    start() : None
        Starts the bot
    """

    _telegram = None
    _settings = None
    _clients_data = None

    def __init__(self, path_settings='config.ini'):
        self._settings = BotSettings(path_settings)
        self._init_clients_data()

    def start(self):
        """Starting the bot"""

        # Logging
        path_logs = self._settings.path_logs()
        logging.basicConfig(
            filename=path_logs,
            format='[%(levelname) 5s/%(asctime)s] %(name)s: %(message)s',
            level=logging.WARNING
        )

        # Initializing
        telegram = TelegramClient(
            self._settings.api_name(),
            self._settings.api_id(),
            self._settings.api_hash()
        )

        managers = self._managers()

        @telegram.on(events.NewMessage(chats=managers))
        async def handler_manager(event):
            await self._handle_manager(event)

        @telegram.on(events.NewMessage(chats=managers, blacklist_chats=True))
        async def handler_client(event):
            await self._handle_client(event)

        self._telegram = telegram
        self._telegram.start()
        self._telegram.run_until_disconnected()

    async def _handle_manager(self, event):
        """Handles events in a manager's chat."""

        # New message
        message = event.message
        manager = message.peer_id.user_id
        text = message.message

        # Get a client from the reply message
        client = 0
        if message.reply_to is not None:
            chat = await message.get_chat()
            reply_id = message.reply_to.reply_to_msg_id
            reply_msg = await self._telegram.get_messages(chat, ids=reply_id)
            if reply_msg is not None:
                try:
                    reply_text = reply_msg.message

                    start_text = 'Клієнт: '
                    end_text = '\n'
                    start = reply_text.find(start_text) + len(start_text)
                    end = reply_text.find(end_text)

                    client = int(reply_text[start:end])
                except:
                    client = 0

        # Start bot
        if text.startswith('/start'):
            welcome_text = 'Онлайн-помічник вітає Вас!\n'
            welcome_text += 'В цей чат будуть надходити звернення від клієнтів.'
            await self._telegram.send_message(manager, welcome_text)

            doc_text = 'При бажанні можете ознайомитись з інструкцією користувача 👆'
            filepath = self._settings.path_doc()
            await self._telegram.send_file(manager, filepath, caption=doc_text)
        # Refresh bot
        elif text.startswith('/refresh'):
            if manager == self._manager_admin():
                old_df = self._clients_data.copy()
                self._init_clients_data()
                new_df = self._clients_data
                for index, row in new_df.iterrows():
                    find_rows = old_df.loc[old_df['id'] == row['id']]
                    if find_rows.empty:
                        continue

                    found_row = find_rows.iloc[0]
                    row['chatting'] = found_row.chatting
                    row['text'] = found_row.text
                    row['documenting'] = found_row.documenting
        # Initiate a conversation with a client
        elif text.startswith('/initiate_task'):
            str_id = text.replace('/initiate_task_', '')
            try:
                client_id = int(str_id)
            except:
                client_id = 0

            if client_id:
                if self._is_chatting(client_id):
                    send_text = 'У клієнта вже є відкрите звернення'
                    await self._telegram.send_message(manager, send_text)
                    return

                # Send an initiate message to the manager
                send_text = 'Клієнт: ' + str(client_id)
                send_text += '\nНапишіть звернення клієнту та відправте його як відповідь на це повідомлення 👇'
                await self._telegram.send_message(manager, send_text)

                # Send an initiate message to the chosen client
                new_text = 'Менеджер розпочав діалог, очікуйте на його звернення...'
                keyboard = Button.force_reply()
                await self._telegram.send_message(client_id, new_text, buttons=keyboard)
                self._set_chatting(client_id, True)
            else:
                # Choose a client
                clients = self._clients_by_manager(manager)
                send_text = 'Оберіть клієнта, щоб відправити йому звернення:\n'
                max_len = 2000
                for el in clients:
                    cl_id = el['id']
                    name = el['name']
                    enterprise = el['enterprise']
                    send_text += f'{enterprise} ({name}) - /initiate_task_{cl_id}\n'
                    if len(send_text) > max_len:
                        await self._telegram.send_message(manager, send_text)
                        send_text = ''
                if send_text:
                    await self._telegram.send_message(manager, send_text)

        # No conversations without a client's ID
        if not client:
            return

        # Finish the conversation
        if text.startswith('/finish_task'):
            if not self._is_chatting(client):
                return

            new_text = 'Звернення закрито менеджером'
            keyboard = Button.text('⤴ Головне меню', resize=True)
            await self._telegram.send_message(client, new_text, buttons=keyboard)
            self._set_chatting(client, False)
        # Continue the conversation
        else:
            filepath = await self._get_media_from_message(message)
            if filepath:
                await self._telegram.send_file(client, filepath, caption=text)
                os.remove(filepath)
            else:
                await self._telegram.send_message(client, text)

    async def _handle_client(self, event):
        """Handles events in a client's chat."""

        # New message
        message = event.message
        client = message.peer_id.user_id
        manager = self._manager_by_client(client)
        text = message.message

        # Global options
        option_back = '⤴ Головне меню'
        option_comment = '⁉ Звернутись'
        option_ask = '⁉ Запитати'

        menu_back = Button.text(option_back, resize=True)
        menu_comment = [Button.text(option_comment, resize=True), menu_back]
        menu_ask = [Button.text(option_ask, resize=True), menu_back]
        menu_reply = Button.force_reply()

        # Main menu
        option_goods = '💊 Товари'
        option_pharmacies = '🏥 Аптеки'
        option_documents = '📑 Документи'
        option_reports = '📈 Звіти'
        option_defects = '🛠 Технічний збій'

        menu_main = [
            [Button.text(option_goods, resize=True), Button.text(option_pharmacies, resize=True)],
            [Button.text(option_documents, resize=True), Button.text(option_reports, resize=True)],
            [Button.text(option_defects, resize=True)]
        ]

        # Goods menu
        option_goods_find = '🔎 Товар не відображається'
        option_goods_add = '📥 Додати новий товар'
        option_goods_link = '🪢 Змінити прив\'язку товара'

        menu_goods = [
            [Button.text(option_goods_find, resize=True)],
            [Button.text(option_goods_add, resize=True)],
            [Button.text(option_goods_link, resize=True)],
            menu_ask
        ]

        # Pharmacies menu
        option_pharmacies_find = '🔎 Аптека не відображається'
        option_pharmacies_reply = '🔄 Відповідь на звернення'
        option_pharmacies_add = '🏥 Додати нову аптеку'
        option_pharmacies_schedule = '📆 Змінити графік'
        option_pharmacies_phone = '☎ Змінити номер'
        option_pharmacies_map = '🗺 Змінити точку'
        option_pharmacies_name = '🆕 Змінити назву'
        option_pharmacies_disable = '🚫 Відключити аптеку'
        option_pharmacies_stop = '❌ Відключити мережу'
        option_pharmacies_client = '📞 Номер клієнта'

        menu_pharmacies = [
            [Button.text(option_pharmacies_find, resize=True), Button.text(option_pharmacies_reply, resize=True)],
            [Button.text(option_pharmacies_add, resize=True), Button.text(option_pharmacies_client, resize=True)],
            [Button.text(option_pharmacies_schedule, resize=True), Button.text(option_pharmacies_phone, resize=True)],
            [Button.text(option_pharmacies_map, resize=True), Button.text(option_pharmacies_name, resize=True)],
            [Button.text(option_pharmacies_disable, resize=True)],
            [Button.text(option_pharmacies_stop, resize=True)],
            menu_ask
        ]

        # Documents menu
        option_documents_contracts = '📜 Договори'
        option_documents_invoices = '🧾 Рахунки'
        option_documents_acts = '📇 Акти'
        option_documents_contact = '👤 Змінити контактну особу'

        menu_documents = [
            [Button.text(option_documents_contracts, resize=True)],
            [Button.text(option_documents_invoices, resize=True), Button.text(option_documents_acts, resize=True)],
            [Button.text(option_documents_contact, resize=True)],
            menu_ask
        ]

        # Reports menu
        option_reports_link = '🪢 Товари без прив\'язки'
        option_reports_quality = '📈 Якість'
        option_reports_competitors = '🗺 Оточення'
        option_reports_finance = '💰 Фінанси'

        menu_reports = [
            [Button.text(option_reports_link, resize=True)],
            [
                Button.text(option_reports_quality, resize=True),
                Button.text(option_reports_competitors, resize=True),
                Button.text(option_reports_finance, resize=True)
            ],
            menu_ask
        ]

        # Defects menu
        option_defects_account = '🖥 Особистий кабінет'
        option_defects_orders = '🛒 Замовлення'
        option_defects_rests = '📦 Залишки'

        menu_defects = [
            [Button.text(option_defects_account, resize=True)],
            [Button.text(option_defects_orders, resize=True), Button.text(option_defects_rests, resize=True)],
            menu_ask
        ]

        # Common text
        text_auth = 'Для початку роботи необхідно авторизуватись.\n' \
                    'Введіть код підприємства 👇'
        text_comment = 'Будь ласка, напишіть Ваше звернення 🖌'
        text_ask = 'Попередження!\n' \
                   'Якщо звернутись до менеджера без вибору типу запитання, ' \
                   'Ваше звернення може оброблятися більш тривалий термін (до 48 годин).\n' + text_comment

        # Default data
        new_text = 'Оберіть, будь ласка, розділ, користуючись кнопками нижче 👇'
        keyboard = menu_comment

        # Previous text
        prev_text = ''
        if message.reply_to is not None:
            reply_id = message.reply_to.reply_to_msg_id
            reply_msg = await self._telegram.get_messages(client, ids=reply_id)
            if reply_msg is not None:
                prev_text = reply_msg.message

        # Respond type
        is_auth = True if prev_text.endswith(text_auth) else False
        is_main = True if text.startswith('/start') or text == option_back else False

        # Auth
        if is_auth:
            try:
                code = int(text)
            except:
                code = 0

            if code and code in self._get_enterprises_from_crm():
                name = ''
                sender = await event.get_sender()
                if sender is not None:
                    first_name = sender.first_name
                    if first_name is not None:
                        name += first_name
                    last_name = sender.last_name
                    if last_name is not None:
                        if name:
                            name += ' '
                        name += last_name
                    user_name = sender.username
                    if user_name is not None:
                        if name:
                            name += ' '
                        name += '(' + user_name + ')'
                manager = self._manager_by_enterprise(code)
                self._set_auth(client, name, code, manager)

                ent_names = self._get_enterprise_name_from_crm(code)
                new_text = 'Ви зареєструвалися як представник підприємства ' + ent_names[0] + '.\n' + new_text
                keyboard = menu_main
            else:
                new_text = 'Невірний код підприємства!\n'
                new_text += '(для уточнення коду зателефонуйте менеджеру)\n\n'
                new_text += text_auth
                keyboard = menu_reply
        # Need auth
        elif not self._is_auth(client):
            new_text = text_auth
            keyboard = menu_reply
        # Task: Conversation
        elif self._is_chatting(client):
            name = self._client_name(client)
            if self._is_documenting(client):
                manager = self._manager_by_documents()
            filepath = await self._get_media_from_message(message)

            send_text = 'Клієнт: ' + str(client)
            send_text += '\nІм\'я: ' + name
            send_text += '\n' + text
            if filepath:
                await self._telegram.send_file(manager, filepath, caption=send_text)
                os.remove(filepath)
            else:
                await self._telegram.send_message(manager, send_text)

            new_text = ''
            keyboard = menu_reply
        # Ask/Comment
        elif prev_text in (text_ask, text_comment):
            name = self._client_name(client)
            enterprise = self._enterprise_by_client(client)
            topic = self._get_last_text(client)
            filepath = await self._get_media_from_message(message)
            if self._is_documenting(client):
                manager = self._manager_by_documents()

            send_text = 'Клієнт: ' + str(client)
            send_text += '\nІм\'я: ' + name
            send_text += '\nПідприємство: ' + str(enterprise)
            send_text += '\nТема: ' + topic
            send_text += '\nТекст: ' + text
            if filepath:
                await self._telegram.send_file(manager, filepath, caption=send_text)
                os.remove(filepath)
            else:
                await self._telegram.send_message(manager, send_text)
            self._set_chatting(client, True)

            max_hours = 48
            # Goods
            if topic == option_goods_find:
                max_hours = 6
            elif topic == option_goods_add:
                max_hours = 24
            elif topic == option_goods_link:
                max_hours = 24
            # Pharmacies
            elif topic == option_pharmacies_find:
                max_hours = 6
            elif topic == option_pharmacies_reply:
                max_hours = 4
            elif topic == option_pharmacies_add:
                max_hours = 24
            elif topic == option_pharmacies_schedule:
                max_hours = 6
            elif topic == option_pharmacies_phone:
                max_hours = 24
            elif topic == option_pharmacies_map:
                max_hours = 24
            elif topic == option_pharmacies_name:
                max_hours = 6
            elif topic == option_pharmacies_disable:
                max_hours = 4
            elif topic == option_pharmacies_stop:
                max_hours = 2
            elif topic == option_pharmacies_client:
                max_hours = 6
            # Documents
            elif topic == option_documents_contracts:
                max_hours = 24
            elif topic == option_documents_invoices:
                max_hours = 6
            elif topic == option_documents_acts:
                max_hours = 24
            elif topic == option_documents_contact:
                max_hours = 24
            # Reports
            elif topic == option_reports_link:
                max_hours = 24
            elif topic == option_reports_quality:
                max_hours = 24
            elif topic == option_reports_competitors:
                max_hours = 24
            elif topic == option_reports_finance:
                max_hours = 24
            # Defects
            elif topic == option_defects_account:
                max_hours = 24
            elif topic == option_defects_orders:
                max_hours = 6
            elif topic == option_defects_rests:
                max_hours = 6

            new_text = f'Звернення відправлено - очікуйте відповідь менеджера (до {max_hours} годин)'
            keyboard = menu_reply
        # Option Ask/Comment
        elif text in (option_ask, option_comment):
            if text == option_ask:
                new_text = text_ask
            else:
                new_text = text_comment
            keyboard = menu_reply
        # Section Main
        elif is_main:
            keyboard = menu_main
            self._set_documenting(client, False)
        # Section Goods
        elif text == option_goods:
            keyboard = menu_goods
        # Goods Option: Find
        elif text == option_goods_find:
            new_text = 'Товар може не відображатися з деяких причин, основні з яких:\n' \
                       ' 🔹 Аптека не надсилає залишки товару\n' \
                       ' 🔹 Ціна резервування товару вища від ціни в аптеці\n' \
                       ' 🔹 Відсутня прив\'язка товарної позиції\n' \
                       ' 🔹 Товар заблокований\n' \
                       ' 🔹 Аптека відключена\n' \
                       'Ви можете знайти причину, користуючись інструкцією за посиланням:\n' \
                       'У разі, якщо причина не виявлена, надішліть звернення менеджеру, ' \
                       'натиснувши кнопку «' + option_comment + '» та вкажіть:\n' \
                       ' 🔹 Назву товару\n' \
                       ' 🔹 Виробника\n' \
                       ' 🔹 Внутрішній код товару\n' \
                       ' 🔹 Серійний номер аптеки'
        # Goods Option: Add
        elif text == option_goods_add:
            new_text = 'Додавання нового товару в каталог можливо ' \
                       'у разі одержання від заявника інформації про товар в наступному форматі:\n' \
                       'Товарна позиція буде введена в каталог, а по факту ' \
                       'введення картки товару в каталог, Вас повідомлять 🔔'
        # Goods Option: Link
        elif text == option_goods_link:
            new_text = 'У разі виявлення некоректної прив\'язки товару, є можливість її відкоригувати, ' \
                       'виконавши дії, описані в інструкції за посиланням:\n' \
                       'Після проведення прив\'язки товару в особистому кабінеті, ' \
                       'вона проходить модерацію і тільки після цього фіксуються зміни 🪢'
        # Section Pharmacies
        elif text == option_pharmacies:
            keyboard = menu_pharmacies
        # Pharmacies Option: Find
        elif text == option_pharmacies_find:
            new_text = 'Припинення відображення аптеки на можливо за таких обставин:\n' \
                       ' 🔹 Аптека відключена в особистому кабінеті\n' \
                       ' 🔹 Своєчасно несплачені рахунки\n' \
                       ' 🔹 Є неотримані/необроблені замовлення\n' \
                       ' 🔹 Відсутнє оновлення інформації по залишкам товарів і цін більше доби\n' \
                       'Самостійно виявити причину можливо, користуючись інструкцією за посиланням:\n' \
                       'Якщо не знайшли відповіді, звертайтесь до менеджера:\n' \
                       'Натисніть кнопку «' + option_comment + '» та вкажіть СЕРІЙНИЙ НОМЕР аптеки 👇'
            # Pharmacies Option: Reply
        elif text == option_pharmacies_reply:
            new_text = 'Натисніть кнопку «' + option_comment + '» та впишіть відповідь на звернення 👇'
        # Pharmacies Option: Add
        elif text == option_pharmacies_add:
            new_text = 'Для того, щоб додати нову аптеку 🏥 з метою її подальшої трансляції, ' \
                       'потрібно виконати дії описані в інструкції за посиланням:\n' \
                       'По факту додавання аптеки в реєстр менеджер по роботі з аптечними мережами ' \
                       'відправить Вам серійний номер цієї аптеки для подальшого вивантаження даних залишків і цін.'
        # Pharmacies Option: Schedule/Phone
        elif text == option_pharmacies_schedule or text == option_pharmacies_phone:
            new_text = 'Змінити 📆 графік роботи аптеки або ☎ телефон можливо, ' \
                       'користуючись інструкцією за посиланням:\n'
        # Pharmacies Option: Map
        elif text == option_pharmacies_map:
            new_text = 'В разі виявлення помилки щодо розташування аптеки на карті 🗺 ' \
                       'можливо змінити точку, користуючись інструкцією за посиланням:\n' \
                       'Після встановлення нової геолокації в особистому кабінеті, ' \
                       'зміни проходять перевірку та, після підтвердження менеджером, фіксуються на карті 📍'
        # Pharmacies Option: Name
        elif text == option_pharmacies_name:
            new_text = 'Для зміни назви аптеки виконайте дії, вказані в інструкції за посиланням:\n'
        # Pharmacies Option: Disable
        elif text == option_pharmacies_disable:
            new_text = 'Відключити аптеку 🚫 від трансляції на сайті можливо самостійно в особистому кабінеті, ' \
                       'користуючись інструкцією за посиланням:\n' \
                       'Якщо аптека відключається на тривалий термін 📆 і в наступному місяці ' \
                       'не планується робота, обов\'язково ПОВІДОМТЕ про це менеджера❗ 👇'
        # Pharmacies Option: Stop
        elif text == option_pharmacies_stop:
            new_text = 'Для відключення мережі ❌ від трансляції, потрібно передати інформацію менеджеру.\n' \
                       'Для цього натисніть кнопку «' + option_comment + '» ' \
                       'та обов\'язково повідомте причину відключення 👇'
        # Pharmacies Option: Client
        elif text == option_pharmacies_client:
            new_text = 'Вкажіть номер броні та причину необхідності надання номера телефона клієнта 👇'
        # Section Documents
        elif text == option_documents:
            keyboard = menu_documents
        # Documents Option: Contracts
        elif text == option_documents_contracts:
            new_text = 'Питання по договорам Ви можете направити менеджеру.\n' \
                       'Для цього натисніть кнопку «' + option_comment + '» та надішліть запитання 👇'
            self._set_documenting(client, True)
        # Documents Option: Invoices
        elif text == option_documents_invoices:
            new_text = 'Питання по рахункам Ви можете направити менеджеру.\n' \
                       'Для цього натисніть кнопку «' + option_comment + '» та надішліть запитання 👇'
            self._set_documenting(client, True)
        # Documents Option: Acts
        elif text == option_documents_acts:
            new_text = 'Питання по актам Ви можете направити менеджеру.\n' \
                       'Для цього натисніть кнопку «' + option_comment + '» та надішліть запитання 👇'
            self._set_documenting(client, True)
        # Documents Option: Contact
        elif text == option_documents_contact:
            new_text = 'При зміні відповідальної особи, ' \
                       'прохання надати інформацію про ПІБ, контактний телефон, e-mail нової контактної особи.\n' \
                       'Натисніть кнопку «' + option_comment + '» та введіть інформацію для відправки даних 👇'
        # Section Reports
        elif text == option_reports:
            keyboard = menu_reports
        # Reports Option: Link
        elif text == option_reports_link:
            new_text = 'Детальна інструкція по роботі зі звітом «Товари без прив\'язки» ' \
                       'та опис полів звіту є за посиланням:\n' \
                       'Якщо Ви не знайшли відповідь на своє запитання, звертайтесь до менеджера:\n' \
                       'Натисніть кнопку «' + option_comment + '», опишіть проблему ✍ та відправте повідомлення. ' \
                       'Менеджер зв\'яжеться з вами в найкоротший термін 👇'
        # Reports Option: Quality
        elif text == option_reports_quality:
            new_text = 'Детальна інструкція по роботі зі звітом «Якість» ' \
                       'та опис полів звіту є за посиланням:\n' \
                       'Якщо Ви не знайшли відповідь на своє запитання, звертайтесь до менеджера:\n' \
                       'Натисніть кнопку «' + option_comment + '», опишіть проблему ✍ та відправте повідомлення. ' \
                       'Менеджер зв\'яжеться з вами в найкоротший термін 👇'
        # Reports Option: Competitors
        elif text == option_reports_competitors:
            new_text = 'Детальна інструкція по роботі зі звітом «Оточення» ' \
                       'та опис полів звіту є за посиланням:\n' \
                       'Якщо Ви не знайшли відповідь на своє запитання, звертайтесь до менеджера:\n' \
                       'Натисніть кнопку «' + option_comment + '», опишіть проблему ✍ та відправте повідомлення. ' \
                       'Менеджер зв\'яжеться з вами в найкоротший термін 👇'
        # Reports Option: Finance
        elif text == option_reports_finance:
            new_text = 'Детальна інструкція по роботі зі звітом «Фінансовий» ' \
                       'та опис полів звіту є за посиланням:\n' \
                       'Якщо Ви не знайшли відповідь на своє запитання, звертайтесь до менеджера:\n' \
                       'Натисніть кнопку «' + option_comment + '», опишіть проблему ✍ та відправте повідомлення. ' \
                       'Менеджер зв\'яжеться з вами в найкоротший термін 👇'
        # Section Defects
        elif text == option_defects:
            keyboard = menu_defects
        # Defects Option: Account
        elif text == option_defects_account:
            new_text = 'Якщо Ви не можете відкрити сторінку 🖥 особистого кабінету, ' \
                       'або зафіксовано ⚠ збій в роботі, будь ласка, оформіть заявку в службу підтримки:\n' \
                       'Натисніть кнопку «' + option_comment + '», опишіть проблему ✍ та відправте повідомлення. ' \
                       'Менеджер зв\'яжеться з вами в найкоротший термін 👇'
        # Defects Option: Orders
        elif text == option_defects_orders:
            new_text = 'Якщо в аптеку не надходять вже сформовані клієнтами 🛒 замовлення, ' \
                       'потрібно звернутись до IT-спеціалістів свого підприємства!\n' \
                       'В разі, якщо технічні спеціалісти аптеки не можуть 😞 вирішити питання, ' \
                       'звертайтесь до менеджера:\n' \
                       'Натисніть кнопку «' + option_comment + '», опишіть проблему ✍ та відправте повідомлення. ' \
                       'Менеджер зв\'яжеться з вами в найкоротший термін 👇'
        # Defects Option: Rests
        elif text == option_defects_rests:
            new_text = 'Перевірити статус надходження 📦 залишків можливо, ' \
                       'виконавши дії згідно інструкціЇ за посиланням:\n' \
                       'Якщо Ви не знайшли відповідь на своє запитання, звертайтесь до менеджера:\n' \
                       'Натисніть кнопку «' + option_comment + '», опишіть проблему ✍ та відправте повідомлення. ' \
                       'Менеджер зв\'яжеться з вами в найкоротший термін 👇'
        # Invalid input response
        else:
            new_text = 'Невірна команда ⚠\n' + new_text
            keyboard = menu_main

        # Setting the client's recent input to memorize a topic
        if text not in (option_ask, option_comment):
            self._set_last_text(client, text)

        # Draw menu
        if new_text:
            await event.reply(new_text, buttons=keyboard)

    async def _get_media_from_message(self, msg):
        user = msg.peer_id.user_id
        media = msg.media
        filepath = ''
        if media is not None:
            filepath = self._settings.path_media(user)
            filepath = await self._telegram.download_media(msg, filepath)

        return filepath

    def _init_clients_data(self):
        """Sets the clients data table"""

        rows = []
        with open(self._settings.path_users(), 'r', encoding='utf-8-sig') as f:
            lines = f.readlines()
            for line in lines:
                data = json.loads(line.replace('\n', ''))
                rows.append(data)

        if len(rows) > 0:
            df = DataFrame(rows)
            df['chatting'] = False
            df['text'] = ''
            df['documenting'] = False
        else:
            df = DataFrame(columns=['id', 'name', 'enterprise', 'manager', 'chatting', 'text', 'documenting'])

        self._clients_data = df

    def _client_name(self, client):
        """Returns a client's name."""

        value = self._get_client_value(client, 'name')
        name = "Анонім" if value is None else value

        return name

    def _enterprise_by_client(self, client):
        """Returns a code client's enterprise."""

        value = self._get_client_value(client, 'enterprise')
        code = 0 if value is None else value

        return int(code)

    def _set_auth(self, client, name, code, manager):
        """Sets the client as authorized"""

        if self._is_auth(client):
            return

        with open(self._settings.path_users(), 'a', encoding='utf-8') as f:
            data = {
                'id': client,
                'name': name,
                'enterprise': code,
                'manager': manager
            }
            str_data = json.dumps(data, ensure_ascii=False) + '\n'
            f.write(str_data)

        data.update({'chatting': False, 'text': '', 'documenting': False})
        new_row = DataFrame([data])
        self._clients_data = self._clients_data.append(new_row)

    def _is_auth(self, client):
        """Returns either the client is authorized or not."""

        value = self._get_client_value(client, 'id')
        is_auth = False if value is None else True

        return is_auth

    def _set_chatting(self, client, is_chatting):
        """Sets the client as chatting"""

        if self._is_chatting(client) == is_chatting:
            return

        self._set_client_value(client, 'chatting', is_chatting)

    def _is_chatting(self, client):
        """Returns either the client is chatting or not."""

        value = self._get_client_value(client, 'chatting')
        is_chatting = False if value is None else value

        return is_chatting

    def _set_last_text(self, client, text):
        """Sets the client's recent input."""

        if self._get_last_text(client) == text:
            return

        self._set_client_value(client, 'text', text)

    def _get_last_text(self, client):
        """Returns the client's recent input."""

        value = self._get_client_value(client, 'text')
        text = '' if value is None else value

        return text

    def _set_documenting(self, client, is_documenting):
        """Sets the client's documents request."""

        if self._is_documenting(client) == is_documenting:
            return

        self._set_client_value(client, 'documenting', is_documenting)

    def _is_documenting(self, client):
        """Returns the client's documents request."""

        value = self._get_client_value(client, 'documenting')
        is_documenting = False if value is None else value

        return is_documenting

    def _set_client_value(self, client, column, value):
        """Updates the clients' data table"""

        df = self._clients_data
        df.loc[df.id == client, column] = value

    def _get_client_value(self, client, column):
        """Returns a column's value from the clients' data table"""

        try:
            df = self._clients_data
            value = df.loc[df.id == client, column].values[0]
        except:
            value = None

        return value

    def _managers(self):
        """Returns all the managers."""

        return self._get_managers_from_crm()

    def _manager_admin(self):
        """Returns admin manager from the settings"""

        return self._settings.admin_manager()

    def _manager_by_default(self):
        """Returns default manager from the settings if a responsible manager isn't found"""

        return self._settings.default_manager()

    def _manager_by_documents(self):
        """Returns documents manager from the settings if a documents response is sent"""

        return self._settings.documents_manager()

    def _manager_by_enterprise(self, code):
        """Returns an id of manager who is responsible for the given enterprise."""

        managers = self._get_managers_from_crm(code)
        if managers:
            manager = managers[0]
        else:
            manager = self._manager_by_default()

        return manager

    def _manager_by_client(self, client):
        """Returns a manager's id who is responsible for the given client."""

        value = self._get_client_value(client, 'manager')
        manager = 0 if value is None else int(value)
        if not manager:
            manager = self._manager_by_default()

        return manager

    def _clients_by_manager(self, manager):
        """Returns clients' id for the given responsible manager."""

        clients = []

        df = self._clients_data
        data = df.loc[df.manager == manager]
        data = data.sort_values(by=['enterprise', 'name', 'id'])
        for index, row in data.iterrows():
            clients.append({'id': row.id, 'name': row[1], 'enterprise': row.enterprise})

        return clients

    def _get_managers_from_crm(self, code=0):
        """Returns filled managers from CRM"""

        managers = []
        query = """
                    SELECT _Fld1111 AS data
                    FROM [DB].[dbo].[_Reference1111] WITH (NOLOCK)
                    WHERE _Fld1111 > 0
                """
        if code:
            query += """
                        AND _Code IN(
                            SELECT _Fld1111 AS name
                            FROM [DB].[dbo].[_Reference1111] WITH (NOLOCK)
                            WHERE _Code = """ + str(code) + ')'

        data = self._get_data_from_crm(query)
        if data:
            for tg_id in data:
                managers.append(int(tg_id))
        else:
            managers.append(self._manager_by_default())

        return managers

    def _get_enterprises_from_crm(self):
        """Returns enterprises from CRM"""

        codes = []
        query = """
                    SELECT _Code AS data
                    FROM [DB].[dbo].[_Reference1111] WITH (NOLOCK)
                    WHERE _Fld1111RRef = 0x11111111111111111111111111111111
                """
        data = self._get_data_from_crm(query)
        if data:
            for code in data:
                codes.append(int(code))
        else:
            codes.append(666)

        return codes

    def _get_enterprise_name_from_crm(self, code):
        """Returns filled managers from CRM"""

        names = []
        query = """
                    SELECT _Description AS data
                    FROM [DB].[dbo].[_Reference1111] WITH (NOLOCK)
                    WHERE _Code = """ + str(code)
        data = self._get_data_from_crm(query)
        if data:
            for name in data:
                names.append(name)
        else:
            names.append(str(code))

        return names

    @staticmethod
    def _get_data_from_crm(query):
        data = []

        try:
            server = 'server'
            db = 'db'
            user = 'user'
            pw = 'password'

            url = 'DRIVER={ODBC Driver 13 for SQL Server};' + f'SERVER={server};DATABASE={db};UID={user};PWD={pw}'
            connection = pyodbc.connect(url)
            cursor = connection.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            for row in rows:
                data.append(row.data)
        except:
            data = []

        return data
