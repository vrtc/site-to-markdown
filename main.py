# Ключевые импорты
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response
from markitdown import MarkItDown, UnsupportedFormatException, FileConversionException
from urllib.parse import unquote, urlparse
import asyncio
import logging
import os
import tempfile
import requests
from io import BytesIO
import chardet

# Инициализация FastAPI приложения
app = FastAPI(
    title="URL to Markdown API",
    description="API service to convert urls to markdown using MarkItDown",
    version="1.0.0",
)

# Константы конфигурации
REQUEST_TIMEOUT = 30  # Таймаут для HTTP запроса (секунды)
CONVERSION_TIMEOUT = 25  # Таймаут для конвертации (секунды)
MAX_CONTENT_SIZE = 10 * 1024 * 1024  # Максимальный размер контента (10 MB)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Эндпоинт для проверки работоспособности сервиса
@app.get("/healthz")
async def healthz():
    return Response(content="ok", media_type="text/plain")


# Основной эндпоинт для конвертации URL в Markdown
@app.get("/{url:path}")
async def convert_url(url: str, request: Request):
    # Извлекаем URL из пути запроса (поддержка query параметров)
    url = (
        request.url.path[1:]
        if not request.url.query
        else request.url.path[1:] + "?" + request.url.query
    )
    logger.info("Received URL path: %s", url)
    
    # Обработка пустого запроса
    if url is None or url == "":
        return Response(
            content="Welcome to URL to Markdown API\nUsage: https://markdown.nimk.ir/YOUR_URL",
            media_type="text/plain",
        )

    # Декодируем URL из формата percent-encoding
    decoded_url = unquote(url)
    logger.info("Decoded URL: %s", decoded_url)

    # Проверка на системные файлы (favicon.ico и т.д.)
    if decoded_url in ("favicon.ico", "robots.txt", "sitemap.xml"):
        logger.info("Skipping system file: %s", decoded_url)
        return Response(
            content="Not Found",
            media_type="text/plain",
            status_code=404
        )

    try:
        # Автоматическое добавление протокола, если его нет
        if not decoded_url.startswith(("http://", "https://")):
            if decoded_url.startswith("www."):
                decoded_url = "https://" + decoded_url
            else:
                decoded_url = "https://www." + decoded_url

        try:
            logger.info("Starting conversion for URL: %s", decoded_url)

            # Асинхронная функция конвертации
            async def _convert() -> str:
                # Блокировка для потокового выполнения (для синхронных операций requests)
                def _run():
                    # Валидация URL: проверка наличия домена
                    parsed = urlparse(decoded_url)
                    if not parsed.netloc:
                        raise ValueError("Invalid URL: no domain specified")
                    
                    logger.info("Downloading content from: %s", decoded_url)
                    
                    # Создаем сессию с User-Agent для имитации браузера
                    session = requests.Session()
                    session.headers.update({
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                    })
                    
                    # Скачиваем контент с потоковой обработкой и ограничением размера
                    response = session.get(decoded_url, timeout=REQUEST_TIMEOUT, stream=True)
                    response.raise_for_status()
                    
                    # Читаем контент чанками с проверкой лимита размера
                    content = b''
                    for chunk in response.iter_content(chunk_size=8192):
                        content += chunk
                        if len(content) > MAX_CONTENT_SIZE:
                            raise ValueError(f"Content size exceeds maximum limit ({MAX_CONTENT_SIZE / 1024 / 1024} MB)")
                    
                    logger.info("Downloaded %d bytes, content-type: %s", len(content), response.headers.get('content-type', 'unknown'))
                    
                    # Инициализация конвертера MarkItDown
                    instance = MarkItDown()
                    
                    # Определяем тип контента и выбираем метод конвертации
                    content_type = response.headers.get('content-type', '')
                    if 'text/html' in content_type:
                        # Для HTML создаем временный файл
                        temp_file = None
                        try:
                            # Определяем кодировку из заголовков или с помощью chardet
                            encoding = 'utf-8'
                            if 'charset=' in content_type:
                                encoding = content_type.split('charset=')[-1].strip()
                            else:
                                # Пытаемся определить кодировку автоматически
                                try:
                                    detected = chardet.detect(content)
                                    encoding = detected.get('encoding', 'utf-8')
                                except Exception:
                                    pass
                            
                            logger.info("Using encoding: %s for HTML content", encoding)
                            
                            with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False, encoding=encoding, errors='replace') as f:
                                f.write(content.decode(encoding, errors='replace'))
                                temp_file = f.name
                            
                            logger.info("Converting HTML from temp file: %s", temp_file)
                            conversion_result = instance.convert_local(temp_file)
                        finally:
                            if temp_file and os.path.exists(temp_file):
                                os.unlink(temp_file)
                                logger.debug("Cleaned up temp file: %s", temp_file)
                    else:
                        # Для других типов используем потоковое чтение
                        logger.info("Converting from stream (content-type: %s)", content_type)
                        conversion_result = instance.convert_stream(BytesIO(content))
                    
                    logger.info("Conversion completed successfully")
                    return conversion_result.text_content

                # Выполняем синхронную функцию в отдельном потоке
                return await asyncio.to_thread(_run)

            # Выполняем конвертацию с таймаутом
            text_content = await asyncio.wait_for(_convert(), timeout=CONVERSION_TIMEOUT)
            return Response(content=text_content, media_type="text/plain")
        
        # Обработка ошибок конвертации
        except UnsupportedFormatException as e:
            logger.error("Unsupported format for URL %s: %s", decoded_url, str(e))
            raise HTTPException(
                status_code=415, detail=f"Unsupported URL format: {str(e)}"
            )
        except FileConversionException as e:
            logger.error("Conversion failed for URL %s: %s", decoded_url, str(e))
            raise HTTPException(
                status_code=400, detail=f"URL conversion failed: {str(e)}"
            )
        except asyncio.TimeoutError:
            logger.error("Conversion timeout for URL: %s", decoded_url)
            raise HTTPException(
                status_code=504, detail="Conversion timed out. Please try again later."
            )
        except requests.RequestException as e:
            logger.error("HTTP request failed for URL %s: %s", decoded_url, str(e))
            raise HTTPException(
                status_code=502, detail=f"Failed to fetch URL: {str(e)}"
            )
        except ValueError as e:
            logger.error("Validation error for URL %s: %s", decoded_url, str(e))
            raise HTTPException(
                status_code=400, detail=str(e)
            )
        except Exception as e:
            logger.exception("Unexpected error processing URL %s: %s", decoded_url, str(e))
            raise HTTPException(
                status_code=500, detail=f"Internal server error: {str(e)}"
            )
    
    # Повторное возбуждение HTTPException (не обрабатывать их как 400)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"URL processing failed: {str(e)}")
