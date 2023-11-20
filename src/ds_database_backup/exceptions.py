import subprocess
from typing import List
from botocore.exceptions import ClientError
from .utils import Utils


class ModuleNotFound(Exception):
    MSG_TEMPLATE = (
        'Ошибка инициализации настроек. Модуль скрипта {module_name} не найден'
    )

    def __init__(self, module_name: str):
        msg = self.MSG_TEMPLATE.format(
            module_name=module_name,
        )
        super().__init__(msg)


class DriveNotExist(Exception):
    MSG_TEMPLATE = (
        'Ошибка инициализации настроек. Параметр {parameter_name} - Корневой диск в {path} не существует'
    )

    def __init__(self, parameter_name: str, path: str):
        msg = self.MSG_TEMPLATE.format(
            parameter_name=parameter_name,
            path=path,
        )
        super().__init__(msg)


class DrivesNotExist(Exception):
    MSG_TEMPLATE = (
        'Ошибка инициализации настроек. В параметрах указаны несуществующие корневые диски:\n{parameter_and_path}'
    )

    def __init__(self, parameter_and_path: {str: str}):
        msg = self.MSG_TEMPLATE.format(
            parameter_and_path='\n'.join(
                [f"Параметр - {key}, путь - {value}" for key, value in parameter_and_path.items()]),
        )
        super().__init__(msg)


class MandatoryPropertiesNotPresent(Exception):
    MSG_TEMPLATE = (
        'Ошибка инициализации настроек. Отстутствуют обязательные параметры - {failed_properties}'
    )

    def __init__(self, failed_properties: List[str]):
        msg = self.MSG_TEMPLATE.format(
            failed_properties=', '.join([f" {value}" for value in failed_properties]),
        )
        super().__init__(msg)


class PathNotExist(Exception):
    MSG_TEMPLATE = (
        'Ошибка инициализации настроек. Пути не существуют:\n{parameter_and_path}'
    )

    def __init__(self, parameter_and_path: {str: str}):
        msg = self.MSG_TEMPLATE.format(
            parameter_and_path='\n'.join(
                [f"Параметр - {key}, путь - {value}" for key, value in parameter_and_path.items()]),
        )
        super().__init__(msg)


class ItsNotFile(Exception):
    MSG_TEMPLATE = (
        'Ошибка инициализации настроек. В параметре должен быть путь к файлу, а найдена папка: {parameter_and_path}'
    )

    def __init__(self, parameter_and_path: {str: str}):
        msg = self.MSG_TEMPLATE.format(
            parameter_and_path='\n'.join(
                [f"Параметр - {key}, путь - {value}" for key, value in parameter_and_path.items()]),
        )
        super().__init__(msg)


class ConfigTypeMismatch(Exception):
    MSG_TEMPLATE = (
        'Ошибка создания модуля {module_name}. Получены аргументы типа {received_type}, а ожидалось - {expected_type}'
    )

    def __init__(self, received_type: str, expected_type: str, module_name: str):
        msg = self.MSG_TEMPLATE.format(
            received_type=received_type,
            expected_type=expected_type,
            module_name=module_name
        )
        super().__init__(msg)


class PgBaseBackupCreateError(Exception):
    MSG_TEMPLATE = (
        'Ошибка при создании полного бэкапа PostgreSQl - {error_text}'
    )

    def __init__(self, error_text: str):
        msg = self.MSG_TEMPLATE.format(
            error_text=error_text,
        )
        super().__init__(msg)


class ArchiveCreateError(Exception):
    MSG_TEMPLATE = (
        'Ошибка при архивировании бэкапа - {error_text}\nКоманда - {command}'
    )

    def __init__(self, exception):
        if isinstance(exception, subprocess.CalledProcessError):
            error_text = Utils.decode_text_or_return_error_msg(exception.stderr)
        else:
            error_text = str(exception)

        msg = self.MSG_TEMPLATE.format(
            error_text=error_text,
            command=exception.cmd
        )
        super().__init__(msg)


class PgDumpRunError(Exception):
    MSG_TEMPLATE = (
        'Ошибка запуска pg_dump - {error_text}\nКоманда - {command}'
    )

    def __init__(self, exception):
        if isinstance(exception, subprocess.CalledProcessError):
            error_text = Utils.decode_text_or_return_error_msg(exception.output)
        else:
            error_text = str(exception)

        msg = self.MSG_TEMPLATE.format(
            error_text=error_text,
            command=exception.cmd
        )
        super().__init__(msg)


class PgDumpCreateError(Exception):
    MSG_TEMPLATE = (
        'Ошибка при создании дампа PostgreSQl - {error_text}'
    )

    def __init__(self, error_text):
        msg = self.MSG_TEMPLATE.format(
            error_text=self._convert_message(error_text),
        )
        super().__init__(msg)

    def _convert_message(self, msg: str) -> str:
        if "invalid page in" in msg:
            return "База повреждена!"
        return msg


class OneCFBBackupCreateError(Exception):
    MSG_TEMPLATE = (
        'Ошибка при создании бэкапа файловой базы 1с - {error_text}'
    )

    def __init__(self, error_text):
        msg = self.MSG_TEMPLATE.format(
            error_text=error_text,
        )
        super().__init__(msg)


class MsSqlCreateError(Exception):
    MSG_TEMPLATE = (
        'Ошибка при создании бэкапа MS SQL - {error_text}'
    )

    def __init__(self, error_text):
        msg = self.MSG_TEMPLATE.format(
            error_text=error_text,
        )
        super().__init__(msg)


class AWSError(Exception):

    def __init__(self, msg: str):
        super().__init__(msg)

    error_codes = [
        {'Code': 'AccessDenied', 'Description': 'Доступ к бакету запрещен'},
        {'Code': 'AccountProblem',
         'Description': 'Проблема с вашей учетной записью AWS, которая мешает успешному завершению действия. '
                        'Обратитесь в службу поддержки AWS для получения дополнительной помощи.'},
        {'Code': 'AllAccessDisabled',
         'Description': 'Весь доступ к этому ресурсу S3 отключен. Обратитесь в службу поддержки AWS для получения '
                        'дополнительной помощи.'},
        {'Code': 'AmbiguousGrantByEmailAddress',
         'Description': 'Указанный вами адрес электронной почты связан с более чем одной учетной записью.'},
        {'Code': 'AuthorizationHeaderMalformed', 'Description': 'Указанный вами заголовок авторизации недействителен.'},
        {'Code': 'BadDigest', 'Description': 'Указанный вами Content-MD5 не совпадает с тем, что мы получили.'},
        {'Code': 'BucketAlreadyExists',
         'Description': 'Запрошенное имя бакета недоступно. Пространство имен бакета общее для всех пользователей '
                        'системы. Пожалуйста, выберите другое имя и попробуйте снова.'},
        {'Code': 'BucketAlreadyOwnedByYou',
         'Description': 'бакет, которое вы пытаетесь создать, уже существует, и вы его владеете. S3 возвращает эту '
                        'ошибку во всех регионах AWS, кроме региона Северная Вирджиния. Для совместимости с '
                        'устаревшими версиями, если вы создаете существующее бакет, которым вы уже владеете в регионе '
                        'Северная Вирджиния, S3 возвращает 200 OK и сбрасывает списки управления доступом к бакету ('
                        'ACL).'},
        {'Code': 'BucketNotEmpty', 'Description': 'бакет, которое вы пытаетесь удалить, не пусто.'},
        {'Code': 'CredentialsNotSupported', 'Description': 'Этот запрос не поддерживает учетные данные.'},
        {'Code': 'CrossLocationLoggingProhibited',
         'Description': 'Запрещено кросс-локационное ведение журнала. бакета в одном географическом местоположении не '
                        'могут вести журнал информации в бакет в другом местоположении.'},
        {'Code': 'EntityTooSmall',
         'Description': 'Ваше предложенное загружаемое содержимое меньше минимально допустимого размера объекта.'},
        {'Code': 'EntityTooLarge',
         'Description': 'Ваше предложенное загружаемое содержимое превышает максимально допустимый размер объекта.'},
        {'Code': 'ExpiredToken', 'Description': 'Предоставленный вами токен истек.'},
        {'Code': 'IllegalVersioningConfigurationException',
         'Description': 'Указывает, что конфигурация версий, указанная в запросе, недействительна.'},
        {'Code': 'IncompleteBody',
         'Description': 'Вы не предоставили количество байтов, указанное в заголовке Content-Length HTTP.'},
        {'Code': 'IncorrectNumberOfFilesInPostRequest',
         'Description': 'POST требует ровно одну загрузку файла на запрос.'},
        {'Code': 'InlineDataTooLarge', 'Description': 'Встроенные данные превышают максимально допустимый размер.'},
        {'Code': 'InternalError',
         'Description': 'Мы столкнулись с внутренней ошибкой. Пожалуйста, попробуйте еще раз.'},
        {'Code': 'InvalidAccessKeyId',
         'Description': 'Предоставленный вами идентификатор ключа доступа AWS не существует в наших записях.'},
        {'Code': 'InvalidAddressingHeader', 'Description': 'Вы должны указать анонимную роль.'},
        {'Code': 'InvalidArgument', 'Description': 'Недопустимый аргумент.'},
        {'Code': 'InvalidBucketName', 'Description': 'Указанное бакет недействительно.'},
        {'Code': 'InvalidBucketState', 'Description': 'Запрос недействителен в текущем состоянии бакета.'},
        {'Code': 'InvalidDigest', 'Description': 'Указанный вами Content-MD5 недействителен.'},
        {'Code': 'InvalidEncryptionAlgorithmError',
         'Description': 'Указанный вами запрос на шифрование недействителен. Допустимое значение - AES256.'},
        {'Code': 'InvalidLocationConstraint', 'Description': 'Указанное ограничение местоположения недействительно.'},
        {'Code': 'InvalidObjectState', 'Description': 'Действие недействительно для текущего состояния объекта.'},
        {'Code': 'InvalidPart',
         'Description': 'Один или несколько указанных частей не могут быть найдены. Часть может не быть загружена, '
                        'или указанный сущностный тег может не соответствовать сущностному тегу части.'},
        {'Code': 'InvalidPartOrder',
         'Description': 'Список частей не был в возрастающем порядке. Список частей должен быть указан в порядке '
                        'номера части.'},
        {'Code': 'InvalidPayer',
         'Description': 'Весь доступ к этому объекту отключен. Пожалуйста, свяжитесь с технической поддержкой AWS для '
                        'получения дополнительной помощи.'},
        {'Code': 'InvalidPolicyDocument',
         'Description': 'Содержание формы не соответствует условиям, указанным в документе политики.'},
        {'Code': 'InvalidRange', 'Description': 'Запрошенный диапазон не может быть удовлетворен.'},
        {'Code': 'InvalidRequest', 'Description': 'Пожалуйста, используйте AWS4-HMAC-SHA256.'},
        {'Code': 'InvalidRequest', 'Description': 'SOAP-запросы должны выполняться через защищенное соединение HTTPS.'},
        {'Code': 'InvalidRequest',
         'Description': 'S3 Transfer Acceleration не поддерживается для бакетов с недопустимыми именами DNS.'},
        {'Code': 'InvalidRequest',
         'Description': 'S3 Transfer Accelerate endpoint поддерживает только виртуальные запросы стиля.'},
        {'Code': 'InvalidRequest', 'Description': 'S3 Transfer Accelerate отключен для этого бакета.'},
        {'Code': 'InvalidRequest',
         'Description': 'S3 Transfer Acceleration не поддерживается для этого бакета. Свяжитесь с технической '
                        'поддержкой AWS для получения дополнительной информации.'},
        {'Code': 'InvalidRequest',
         'Description': 'S3 Transfer Acceleration не может быть включен для этого бакета. Свяжитесь с технической '
                        'поддержкой AWS для получения дополнительной информации.'},
        {'Code': 'InvalidSecurity', 'Description': 'Предоставленные вами учетные данные безопасности недействительны.'},
        {'Code': 'InvalidSOAPRequest', 'Description': 'Тело SOAP-запроса недействительно.'},
        {'Code': 'InvalidStorageClass', 'Description': 'Указанный вами класс хранения недействителен.'},
        {'Code': 'InvalidTargetBucketForLogging',
         'Description': 'Целевое бакет для ведения журнала не существует, не является вашим бакетм или не имеет '
                        'соответствующих разрешений для группы доставки журнала.'},
        {'Code': 'InvalidToken',
         'Description': 'Предоставленный вами токен имеет неверный формат или недействителен по другой причине.'},
        {'Code': 'InvalidURI', 'Description': 'Не удалось разобрать указанный URI.'},
        {'Code': 'KeyTooLongError', 'Description': 'Ваш ключ слишком длинный.'},
        {'Code': 'MalformedACLError',
         'Description': 'XML, предоставленный вами, был некорректно сформирован или не прошел проверку по нашей '
                        'опубликованной схеме.'},
        {'Code': 'MalformedPOSTRequest',
         'Description': 'Тело вашего POST-запроса не является корректным многокомпонентным/multipart форматом данных.'},
        {'Code': 'MalformedXML',
         'Description': 'Это происходит, когда пользователь отправляет исказленный XML (XML, не соответствующий '
                        'опубликованной XSD) для конфигурации. Сообщение об ошибке: "XML, предоставленный вами, '
                        'был некорректно сформирован или не прошел проверку по нашей опубликованной схеме".'},
        {'Code': 'MaxMessageLengthExceeded', 'Description': 'Ваш запрос был слишком большим.'},
        {'Code': 'MaxPostPreDataLengthExceededError',
         'Description': 'Поля POST-запроса перед файлом загрузки были слишком большими.'},
        {'Code': 'MetadataTooLarge',
         'Description': 'Ваши заголовки метаданных превышают максимально допустимый размер метаданных.'},
        {'Code': 'MethodNotAllowed', 'Description': 'Указанный метод не разрешен для этого ресурса.'},
        {'Code': 'MissingAttachment', 'Description': 'Ожидалось SOAP-вложение, но ни одно не было найдено.'},
        {'Code': 'MissingContentLength', 'Description': 'Вы должны предоставить заголовок Content-Length HTTP.'},
        {'Code': 'MissingRequestBodyError',
         'Description': 'Это происходит, когда пользователь отправляет пустой XML-документ в запросе. Сообщение об '
                        'ошибке: "Тело запроса пусто".'},
        {'Code': 'MissingSecurityElement', 'Description': 'Запрос SOAP 1.1 не содержит элемента безопасности.'},
        {'Code': 'MissingSecurityHeader', 'Description': 'Ваш запрос не содержит обязательного заголовка.'},
        {'Code': 'NoLoggingStatusForKey', 'Description': 'Нет такой вещи как ведение журнала подресурса для ключа.'},
        {'Code': 'NoSuchBucket', 'Description': 'Указанный бакет не существует.'},
        {'Code': 'NoSuchBucketPolicy', 'Description': 'Указанный бакет не имеет политики бакета.'},
        {'Code': 'NoSuchKey', 'Description': 'Указанный ключ не существует.'},
        {'Code': 'NoSuchLifecycleConfiguration', 'Description': 'Конфигурация жизненного цикла не существует.'},
        {'Code': 'NoSuchUpload',
         'Description': 'Указанная многокомпонентная загрузка не существует. Идентификатор загрузки может быть '
                        'недействителен, или многокомпонентная загрузка могла быть прервана или завершена.'},
        {'Code': 'NoSuchVersion',
         'Description': 'Указанный в запросе идентификатор версии не соответствует существующей версии.'},
        {'Code': 'NotImplemented',
         'Description': 'Предоставленный вами заголовок подразумевает функциональность, которая не реализована.'},
        {'Code': 'NotSignedUp',
         'Description': 'Ваша учетная запись не зарегистрирована для услуги S3. Вы должны зарегистрироваться, '
                        'прежде чем использовать S3. Вы можете зарегистрироваться по следующему URL-адресу: S3'},
        {'Code': 'OperationAborted',
         'Description': 'Противоречивое условное действие в настоящее время выполняется против этого ресурса. '
                        'Попробуйте еще раз.'},
        {'Code': 'PermanentRedirect',
         'Description': 'бакет, к которому вы пытаетесь получить доступ, должно быть указано с использованием '
                        'указанной конечной точки. Отправляйте все последующие запросы на эту конечную точку.'},
        {'Code': 'PreconditionFailed', 'Description': 'По крайней мере, одно из указанных предусловий не выполняется.'},
        {'Code': 'Redirect', 'Description': 'Временное перенаправление.'},
        {'Code': 'RestoreAlreadyInProgress', 'Description': 'Восстановление объекта уже выполняется.'},
        {'Code': 'RequestIsNotMultiPartContent',
         'Description': 'POST бакета должен быть в виде многокомпонентного/multipart формата данных.'},
        {'Code': 'RequestTimeout',
         'Description': 'Соединение с сервером не было прочитано или записано в пределах периода времени ожидания.'},
        {'Code': 'RequestTimeTooSkewed',
         'Description': 'Разница между локальным временем и временем сервера слишком велика.'},
        {'Code': 'RequestTorrentOfBucketError', 'Description': 'Запрос файла торрента бакета не разрешен.'},
        {'Code': 'SignatureDoesNotMatch',
         'Description': 'Рассчитанная нами подпись запроса не соответствует предоставленной вами подписи. Проверьте '
                        'свой секретный ключ доступа AWS и метод подписи. Дополнительные сведения см. в разделе '
                        'Аутентификация REST и аутентификация SOAP для получения подробной информации.'},
        {'Code': 'ServiceUnavailable', 'Description': 'Уменьшите скорость запроса.'},
        {'Code': 'SlowDown', 'Description': 'Уменьшите скорость запроса.'},
        {'Code': 'TemporaryRedirect', 'Description': 'Вы перенаправляетесь к бакету во время обновления DNS.'},
        {'Code': 'TokenRefreshRequired', 'Description': 'Предоставленный вами токен должен быть обновлен.'},
        {'Code': 'TooManyBuckets', 'Description': 'Вы пытались создать больше бакетов, чем разрешено.'},
        {'Code': 'UnexpectedContent', 'Description': 'Этот запрос не поддерживает контент.'},
        {'Code': 'UnresolvableGrantByEmailAddress',
         'Description': 'Указанный вами адрес электронной почты не соответствует ни одному аккаунту в наших записях.'},
        {'Code': 'UserKeyMustBeSpecified',
         'Description': 'POST бакета должен содержать указанное имя поля. Если оно указано, проверьте порядок полей.'},
    ]


class AWSConnectionError(AWSError):
    MSG_TEMPLATE = (
        'Ошибка соединения с облаком - {error_description}'
    )

    def __init__(self, error):
        if isinstance(error, ClientError):
            error_code = error.response.get('Error').get('Code')
            error_description = error.response.get('Error').get('Description')
            for pair in super(AWSConnectionError, self).error_codes:
                if pair['Code'] == error_code:
                    error_description = pair['Description']
                    break
        else:
            error_description = str(error)

        msg = self.MSG_TEMPLATE.format(
            error_description=error_description,
        )
        super().__init__(msg)


class RansomwareVirusTracesFound(Exception):
    MSG_TEMPLATE = (
        'Выгрузка в облако была выполнена, '
        'но обнаружены файлы с нетипичными расширениями (вирус-шифровальщик?): \n{corrupt_files}'
    )

    def __init__(self, corrupt_files: List[str]):
        msg = self.MSG_TEMPLATE.format(
            corrupt_files='\n'.join([f" {value}" for value in corrupt_files]),
        )
        super().__init__(msg)


class AWSSpeedAutoAdjustmentError(Exception):
    MSG_TEMPLATE = (
        'Ошибка при синхронизации с облаком - нестабильный интернет, автопонижение скорости выгрузки до'
        ' {speed_kbit_s} Кбит/с не решило проблему'
    )

    def __init__(self, speed_bytes_per_s):
        speed_kbit_s = round(speed_bytes_per_s / 125)
        msg = self.MSG_TEMPLATE.format(
            speed_kbit_s=speed_kbit_s,
        )
        super().__init__(msg)
