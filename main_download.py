import subprocess
import sys
import os

def install_dependencies():
    packages = ['requests', 'tqdm', 'colorama']
    for package in packages:
        try:
            __import__(package)
        except ImportError:
            print(f"Installation de {package}...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", package, "-q"])

install_dependencies()

import requests
import json
from datetime import datetime
import time
from tqdm import tqdm
from colorama import init, Fore, Style
import logging
from typing import List, Dict, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
from pathlib import Path
from threading import Lock
from dataclasses import dataclass, field

init(autoreset=True)

if sys.platform == "win32":
    os.system("chcp 65001 > nul")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('discord_downloader.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

RATE_LIMIT_PAUSE = 0.1
MESSAGE_BATCH_SIZE = 100
MAX_WORKERS = 3
MAX_RETRIES = 5
CHECKPOINT_FILE = "download_checkpoint.json"
ATTACHMENT_DIR = "attachments"


@dataclass
class ThreadSafeStats:
    _lock: Lock = field(default_factory=Lock, repr=False)
    total_messages: int = 0
    total_conversations: int = 0
    total_attachments: int = 0
    start_time: float = field(default_factory=time.time)
    
    def increment_messages(self, count: int = 1):
        with self._lock:
            self.total_messages += count
    
    def increment_conversations(self, count: int = 1):
        with self._lock:
            self.total_conversations += count
    
    def increment_attachments(self, count: int = 1):
        with self._lock:
            self.total_attachments += count
    
    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "total_messages": self.total_messages,
                "total_conversations": self.total_conversations,
                "total_attachments": self.total_attachments,
                "start_time": self.start_time
            }


class DiscordDMDownloader:
    def __init__(self, token: str):
        self.token = token
        self.base_url = "https://discord.com/api/v10"
        self.headers = {
            "Authorization": token,
            "Content-Type": "application/json"
        }
        self.output_dir = "conversations_discord"
        self.attachment_dir = ATTACHMENT_DIR
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=20)
        self.session.mount('https://', adapter)
        self.checkpoint = self.load_checkpoint()
        self.stats = ThreadSafeStats()
        self._message_counts: Dict[str, int] = {}
        self._message_counts_lock = Lock()
        
        Path(self.output_dir).mkdir(exist_ok=True)
        Path(self.attachment_dir).mkdir(exist_ok=True)
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()
        return False
    
    def load_checkpoint(self) -> Dict:
        try:
            if os.path.exists(CHECKPOINT_FILE):
                with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"Impossible de charger le checkpoint: {e}")
        return {}
    
    def save_checkpoint(self, channel_id: str, message_count: int):
        self.checkpoint[channel_id] = {
            "message_count": message_count,
            "timestamp": datetime.now().isoformat()
        }
        try:
            with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
                json.dump(self.checkpoint, f, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Erreur sauvegarde checkpoint: {e}")
    
    def verify_token(self) -> bool:
        try:
            response = self.session.get(f"{self.base_url}/users/@me", timeout=10)
            if response.status_code == 401:
                print(f"{Fore.RED}❌ Token invalide ou expiré{Style.RESET_ALL}")
                logger.error("Token invalide")
                return False
            elif response.status_code == 403:
                print(f"{Fore.RED}❌ Accès refusé{Style.RESET_ALL}")
                logger.error("Accès refusé avec le token")
                return False
            response.raise_for_status()
            user = response.json()
            if not all(k in user for k in ['username', 'id']):
                logger.error("Réponse API invalide")
                return False
            print(f"{Fore.GREEN}✅ Token valide - Connecté: {user['username']}#{user.get('discriminator', '0')}{Style.RESET_ALL}")
            logger.info(f"Connecté: {user['username']}#{user.get('discriminator', '0')}")
            return True
        except requests.exceptions.Timeout:
            print(f"{Fore.RED}❌ Timeout de connexion{Style.RESET_ALL}")
            logger.error("Timeout lors de la vérification")
            return False
        except requests.exceptions.RequestException as e:
            print(f"{Fore.RED}❌ Erreur de connexion: {e}{Style.RESET_ALL}")
            logger.error(f"Erreur connexion: {e}")
            return False
    
    def handle_rate_limit(self, response: requests.Response) -> float:
        if response.status_code == 429:
            retry_data = response.json()
            retry_after = retry_data.get('retry_after', 5)
            print(f"{Fore.YELLOW}⏳ Rate limit - attente {retry_after:.1f}s{Style.RESET_ALL}")
            logger.warning(f"Rate limit: attente de {retry_after}s")
            return retry_after + 0.5
        return 0
    
    def get_private_channels(self, retries: int = MAX_RETRIES) -> List[Dict]:
        for attempt in range(retries):
            try:
                response = self.session.get(f"{self.base_url}/users/@me/channels", timeout=10)
                
                wait_time = self.handle_rate_limit(response)
                if wait_time > 0:
                    if attempt < retries - 1:
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error("Rate limit persistant sur get_private_channels")
                        return []
                
                if response.status_code == 403:
                    logger.error("Accès refusé aux canaux privés")
                    return []
                
                response.raise_for_status()
                channels = response.json()
                
                if not isinstance(channels, list):
                    logger.error("Format de réponse invalide pour les canaux")
                    return []
                
                return channels
            except requests.exceptions.RequestException as e:
                logger.error(f"Erreur récupération canaux (tentative {attempt + 1}/{retries}): {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                return []
        return []
    
    def get_messages(self, channel_id: str, before_id: Optional[str] = None, retries: int = MAX_RETRIES) -> List[Dict]:
        for attempt in range(retries):
            try:
                url = f"{self.base_url}/channels/{channel_id}/messages?limit={MESSAGE_BATCH_SIZE}"
                if before_id:
                    url += f"&before={before_id}"
                
                response = self.session.get(url, timeout=15)
                
                wait_time = self.handle_rate_limit(response)
                if wait_time > 0:
                    if attempt < retries - 1:
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"Rate limit persistant sur canal {channel_id}")
                        return []
                
                if response.status_code == 403:
                    logger.warning(f"Accès refusé au canal {channel_id}")
                    return []
                elif response.status_code == 404:
                    logger.warning(f"Canal {channel_id} non trouvé")
                    return []
                elif response.status_code >= 500:
                    logger.error(f"Erreur serveur Discord (5xx) pour canal {channel_id}")
                    if attempt < retries - 1:
                        time.sleep(2 ** attempt)
                        continue
                    return []
                
                response.raise_for_status()
                messages = response.json()
                
                if not isinstance(messages, list):
                    logger.error(f"Format de réponse invalide pour canal {channel_id}")
                    return []
                
                return messages
            except requests.exceptions.Timeout:
                logger.error(f"Timeout pour canal {channel_id} (tentative {attempt + 1}/{retries})")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                return []
            except requests.exceptions.RequestException as e:
                logger.error(f"Erreur récupération messages (tentative {attempt + 1}/{retries}): {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                return []
        return []
    
    def download_attachment(self, url: str, filename: str, channel_id: str) -> Optional[str]:
        try:
            channel_dir = Path(self.attachment_dir) / channel_id
            channel_dir.mkdir(exist_ok=True)
            
            safe_filename = "".join(c for c in filename if c.isalnum() or c in "._- ")[:200]
            filepath = channel_dir / safe_filename
            
            if filepath.exists():
                return str(filepath)
            
            with self.session.get(url, timeout=30, stream=True) as response:
                response.raise_for_status()
                
                with open(filepath, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
            
            self.stats.increment_attachments()
            return str(filepath)
        except Exception as e:
            logger.error(f"Erreur téléchargement attachment {filename}: {e}")
            return None
    
    def get_all_messages(self, channel_id: str, user_display_name: str) -> List[Dict]:
        all_messages = []
        before_id = None
        last_count = 0
        no_progress_count = 0
        
        with tqdm(desc=f"{Fore.CYAN}📥 {user_display_name}{Style.RESET_ALL}", 
                  unit=" msgs", ncols=100, 
                  bar_format='{desc}: {n_fmt} messages') as pbar:
            
            while True:
                messages = self.get_messages(channel_id, before_id)
                
                if not messages:
                    break
                
                all_messages.extend(messages)
                pbar.update(len(messages))
                
                if len(all_messages) == last_count:
                    no_progress_count += 1
                    if no_progress_count > 3:
                        logger.warning(f"Pas de progression pour {user_display_name}, arrêt")
                        break
                else:
                    no_progress_count = 0
                    last_count = len(all_messages)
                
                before_id = messages[-1]['id']
                
                time.sleep(RATE_LIMIT_PAUSE)
                
                if len(messages) < MESSAGE_BATCH_SIZE:
                    break
                
                if len(all_messages) % 1000 == 0:
                    self.save_checkpoint(channel_id, len(all_messages))
        
        with self._message_counts_lock:
            self._message_counts[channel_id] = len(all_messages)
        
        all_messages.reverse()
        return all_messages
    
    def format_message(self, msg: Dict, channel_id: str) -> Dict:
        attachments_info = []
        for att in msg.get('attachments', []):
            local_path = None
            if att.get('url'):
                local_path = self.download_attachment(att['url'], att['filename'], channel_id)
            
            attachments_info.append({
                "filename": att['filename'],
                "url": att['url'],
                "size": att.get('size', 0),
                "local_path": local_path
            })
        
        return {
            "id": msg['id'],
            "timestamp": msg['timestamp'],
            "edited_timestamp": msg.get('edited_timestamp'),
            "author": {
                "id": msg['author']['id'],
                "username": msg['author']['username'],
                "discriminator": msg['author'].get('discriminator', '0'),
                "global_name": msg['author'].get('global_name')
            },
            "content": msg['content'],
            "attachments": attachments_info,
            "embeds": msg.get('embeds', []),
            "reactions": msg.get('reactions', []),
            "message_reference": msg.get('message_reference'),
            "stickers": msg.get('sticker_items', [])
        }
    
    def sanitize_filename(self, name: str) -> str:
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            name = name.replace(char, '_')
        name = "".join(c for c in name if ord(c) < 128 or c.isalnum() or c in "._- ")
        return name[:100]
    
    def should_update_conversation(self, filepath: str, channel_id: str) -> bool:
        if not os.path.exists(filepath):
            return True
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
                test_messages = self.get_messages(channel_id)
                if not test_messages:
                    return False
                
                newest_id = test_messages[0]['id']
                if 'messages' in data and data['messages']:
                    last_id = data['messages'][-1]['id']
                    if newest_id != last_id:
                        return True
                
                return False
        except Exception as e:
            logger.error(f"Erreur vérification mise à jour: {e}")
            return True
    
    def save_conversation_streaming(self, filepath: str, conversation_data: Dict):
        temp_file = filepath + ".tmp"
        try:
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(conversation_data, f, ensure_ascii=False)
            
            if os.path.exists(filepath):
                backup = filepath + ".bak"
                os.replace(filepath, backup)
            
            os.replace(temp_file, filepath)
            
            if os.path.exists(filepath + ".bak"):
                os.remove(filepath + ".bak")
                
        except Exception as e:
            logger.error(f"Erreur sauvegarde: {e}")
            if os.path.exists(temp_file):
                os.remove(temp_file)
            raise
    
    def download_conversation(self, channel: Dict, update_mode: bool = False) -> bool:
        try:
            channel_type = channel.get('type')
            
            if channel_type == 1:
                if not channel.get('recipients') or len(channel['recipients']) == 0:
                    logger.warning(f"Canal DM sans destinataire: {channel.get('id')}")
                    return False
                recipient = channel['recipients'][0]
                filename = f"{recipient['username']}.json"
                display_name = recipient.get('global_name') or recipient['username']
            elif channel_type == 3:
                if channel.get('name'):
                    filename = f"group_{channel['name']}.json"
                    display_name = f"Groupe: {channel['name']}"
                else:
                    usernames = [r['username'] for r in channel.get('recipients', [])]
                    if not usernames:
                        logger.warning(f"Groupe sans participants: {channel.get('id')}")
                        return False
                    filename = f"group_{'_'.join(usernames[:3])}.json"
                    display_name = f"Groupe: {', '.join(usernames[:3])}"
            else:
                logger.info(f"Type de canal non géré: {channel_type}")
                return False
            
            filename = self.sanitize_filename(filename)
            filepath = os.path.join(self.output_dir, filename)
            
            if update_mode and not self.should_update_conversation(filepath, channel['id']):
                print(f"{Fore.YELLOW}⏭️  {display_name}: Déjà à jour{Style.RESET_ALL}")
                return False
            
            messages = self.get_all_messages(channel['id'], display_name)
            
            if not messages:
                print(f"{Fore.YELLOW}⚠️  {display_name}: Aucun message{Style.RESET_ALL}")
                logger.info(f"Aucun message pour {display_name}")
                return False
            
            conversation_data = {
                "channel_id": channel['id'],
                "channel_type": "DM" if channel_type == 1 else "Group DM",
                "participants": [
                    {
                        "id": r['id'],
                        "username": r['username'],
                        "global_name": r.get('global_name')
                    } for r in channel.get('recipients', [])
                ],
                "total_messages": len(messages),
                "first_message_date": messages[0]['timestamp'] if messages else None,
                "last_message_date": messages[-1]['timestamp'] if messages else None,
                "downloaded_at": datetime.now().isoformat(),
                "messages": [self.format_message(msg, channel['id']) for msg in messages]
            }
            
            self.save_conversation_streaming(filepath, conversation_data)
            
            self.stats.increment_messages(len(messages))
            self.stats.increment_conversations()
            
            print(f"{Fore.GREEN}✅ {display_name}: {len(messages)} messages → {filename}{Style.RESET_ALL}")
            logger.info(f"Sauvegardé: {display_name} ({len(messages)} messages)")
            return True
            
        except KeyboardInterrupt:
            raise
        except Exception as e:
            logger.error(f"Erreur download conversation: {e}", exc_info=True)
            print(f"{Fore.RED}❌ Erreur: {str(e)[:100]}{Style.RESET_ALL}")
            return False
    
    def download_all_parallel(self, dm_channels: List[Dict], update_mode: bool = False):
        print(f"\n{Fore.CYAN}{'=' * 60}{Style.RESET_ALL}")
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(self.download_conversation, channel, update_mode): i 
                      for i, channel in enumerate(dm_channels, 1)}
            
            for future in as_completed(futures):
                idx = futures[future]
                try:
                    future.result()
                except KeyboardInterrupt:
                    executor.shutdown(wait=False, cancel_futures=True)
                    raise
                except Exception as e:
                    logger.error(f"Erreur conversation {idx}: {e}")
    
    def estimate_download_time(self, dm_channels: List[Dict]) -> Dict[str, Any]:
        sample_size = min(3, len(dm_channels))
        sample_channels = dm_channels[:sample_size]
        
        sample_message_counts = []
        print(f"{Fore.CYAN}📊 Analyse des conversations pour estimation...{Style.RESET_ALL}")
        
        for channel in sample_channels:
            try:
                messages = self.get_messages(channel['id'])
                if messages:
                    first_batch_count = len(messages)
                    if first_batch_count >= MESSAGE_BATCH_SIZE:
                        estimated_total = first_batch_count * 10
                    else:
                        estimated_total = first_batch_count
                    sample_message_counts.append(estimated_total)
                else:
                    sample_message_counts.append(0)
                time.sleep(RATE_LIMIT_PAUSE)
            except Exception as e:
                logger.warning(f"Erreur estimation: {e}")
                sample_message_counts.append(100)
        
        if sample_message_counts:
            avg_messages_per_conv = sum(sample_message_counts) / len(sample_message_counts)
        else:
            avg_messages_per_conv = 100
        
        total_estimated_messages = int(avg_messages_per_conv * len(dm_channels))
        
        batches_per_conv = max(1, avg_messages_per_conv / MESSAGE_BATCH_SIZE)
        time_per_batch = RATE_LIMIT_PAUSE + 0.5
        time_per_conv = batches_per_conv * time_per_batch
        
        parallel_factor = min(MAX_WORKERS, len(dm_channels))
        total_seconds = (time_per_conv * len(dm_channels)) / parallel_factor
        
        total_seconds *= 1.3
        
        return {
            "total_conversations": len(dm_channels),
            "estimated_messages": total_estimated_messages,
            "avg_messages_per_conv": int(avg_messages_per_conv),
            "estimated_seconds": int(total_seconds),
            "estimated_minutes": int(total_seconds / 60),
            "sample_size": sample_size
        }
    
    def print_statistics(self):
        stats = self.stats.get_stats()
        elapsed = time.time() - stats["start_time"]
        print(f"\n{Fore.CYAN}{'=' * 60}{Style.RESET_ALL}")
        print(f"{Fore.GREEN}✨ Téléchargement terminé !{Style.RESET_ALL}")
        print(f"{Fore.YELLOW}📊 Statistiques:{Style.RESET_ALL}")
        print(f"  • Conversations: {stats['total_conversations']}")
        print(f"  • Messages: {stats['total_messages']}")
        print(f"  • Attachments: {stats['total_attachments']}")
        print(f"  • Durée: {elapsed:.1f}s")
        if elapsed > 0:
            print(f"  • Vitesse: {stats['total_messages']/elapsed:.1f} msg/s")
        print(f"{Fore.CYAN}📁 Dossier: {self.output_dir}/{Style.RESET_ALL}")
        logger.info(f"Terminé: {stats['total_conversations']} conv, {stats['total_messages']} msg")
    
    def preview_conversations(self, dm_channels: List[Dict]):
        print(f"\n{Fore.CYAN}📋 Aperçu des conversations:{Style.RESET_ALL}\n")
        
        for i, channel in enumerate(dm_channels[:20], 1):
            if channel['type'] == 1:
                recipient = channel['recipients'][0]
                name = recipient.get('global_name') or recipient['username']
                print(f"  {i}. {Fore.GREEN}[DM]{Style.RESET_ALL} {name}")
            elif channel['type'] == 3:
                if channel.get('name'):
                    name = channel['name']
                else:
                    usernames = [r['username'] for r in channel.get('recipients', [])]
                    name = ', '.join(usernames[:3])
                print(f"  {i}. {Fore.YELLOW}[Groupe]{Style.RESET_ALL} {name}")
        
        if len(dm_channels) > 20:
            print(f"\n  ... et {len(dm_channels) - 20} autres")
        
        print()
    
    def download_all(self):
        print(f"{Fore.CYAN}🚀 Démarrage du téléchargement{Style.RESET_ALL}\n")
        
        if not self.verify_token():
            return
        
        print(f"\n{Fore.CYAN}📡 Récupération des conversations...{Style.RESET_ALL}")
        channels = self.get_private_channels()
        
        dm_channels = [c for c in channels if c.get('type') in [1, 3]]
        
        print(f"{Fore.GREEN}📊 {len(dm_channels)} conversation(s) trouvée(s){Style.RESET_ALL}")
        
        if not dm_channels:
            print(f"{Fore.YELLOW}⚠️  Aucune conversation privée{Style.RESET_ALL}")
            return
        
        self.preview_conversations(dm_channels)
        
        estimation = self.estimate_download_time(dm_channels)
        print(f"\n{Fore.YELLOW}⏱️  Estimation basée sur {estimation['sample_size']} conversation(s) analysée(s):{Style.RESET_ALL}")
        print(f"  • Messages estimés: ~{estimation['estimated_messages']:,}")
        print(f"  • Moyenne par conversation: ~{estimation['avg_messages_per_conv']} messages")
        print(f"  • Durée estimée: ~{estimation['estimated_minutes']} min {estimation['estimated_seconds'] % 60} sec")
        
        print(f"\n{Fore.CYAN}Options:{Style.RESET_ALL}")
        print(f"  {Fore.GREEN}1.{Style.RESET_ALL} Tout télécharger")
        print(f"  {Fore.YELLOW}2.{Style.RESET_ALL} Mode incrémental (màj uniquement)")
        print(f"  {Fore.RED}3.{Style.RESET_ALL} Annuler")
        
        choice = input(f"\n{Fore.CYAN}Votre choix [1-3]: {Style.RESET_ALL}").strip()
        
        if choice == '3':
            print(f"{Fore.YELLOW}❌ Annulé{Style.RESET_ALL}")
            return
        
        update_mode = (choice == '2')
        
        try:
            self.download_all_parallel(dm_channels, update_mode)
            self.print_statistics()
        except KeyboardInterrupt:
            print(f"\n\n{Fore.RED}⚠️  Interruption (Ctrl+C){Style.RESET_ALL}")
            print(f"{Fore.YELLOW}💾 Checkpoint sauvegardé{Style.RESET_ALL}")
            logger.warning("Interruption utilisateur")
            sys.exit(0)


def main():
    try:
        print(f"{Fore.CYAN}{'=' * 60}{Style.RESET_ALL}")
        print(f"{Fore.GREEN}  DISCORD DM DOWNLOADER{Style.RESET_ALL}")
        print(f"{Fore.CYAN}{'=' * 60}{Style.RESET_ALL}\n")
        
        token = input(f"{Fore.CYAN}🔑 Token Discord: {Style.RESET_ALL}").strip()
        
        if not token:
            print(f"{Fore.RED}❌ Token vide{Style.RESET_ALL}")
            return
        
        with DiscordDMDownloader(token) as downloader:
            downloader.download_all()
        
    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}Programme interrompu{Style.RESET_ALL}")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"Erreur fatale: {e}", exc_info=True)
        print(f"{Fore.RED}❌ Erreur critique: {e}{Style.RESET_ALL}")
        sys.exit(1)


if __name__ == "__main__":
    main()