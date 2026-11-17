"""Build an editable BPMN 2.0 collaboration with two linked diagram views."""
from pathlib import Path
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parent
NS = {
    'bpmn': 'http://www.omg.org/spec/BPMN/20100524/MODEL',
    'bpmndi': 'http://www.omg.org/spec/BPMN/20100524/DI',
    'dc': 'http://www.omg.org/spec/DD/20100524/DC',
    'di': 'http://www.omg.org/spec/DD/20100524/DI',
    'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
}
for prefix, uri in NS.items():
    ET.register_namespace(prefix, uri)


def element(parent, kind, **attrs):
    prefix, name = kind.split(':')
    return ET.SubElement(parent, f'{{{NS[prefix]}}}{name}', {k: str(v) for k, v in attrs.items()})


definitions = ET.Element(f'{{{NS["bpmn"]}}}definitions', {
    'id': 'Definitions_Search', 'targetNamespace': 'https://example.org/search-reconstruction',
    'name': 'Формирование поисковой страницы',
})
element(definitions, 'bpmn:message', id='Message_Request', name='Поисковый запрос и контекст')
element(definitions, 'bpmn:message', id='Message_Page', name='Готовая страница')
element(definitions, 'bpmn:message', id='Message_Decision', name='Предложения, решение и версии моделей')
element(definitions, 'bpmn:message', id='Message_Actions', name='Показы, клики и действия')
element(definitions, 'bpmn:error', id='Error_Service', name='Ошибка вызова модели или сервиса', errorCode='SERVICE_FAILURE')


class Diagram:
    def __init__(self, process_id, title, collaboration=False):
        self.process = element(definitions, 'bpmn:process', id=process_id, name=title, isExecutable='false')
        self.collaboration = None
        plane_ref = process_id
        if collaboration:
            self.collaboration = element(definitions, 'bpmn:collaboration', id=f'{process_id}_Collaboration')
            plane_ref = self.collaboration.attrib['id']
        diagram = element(definitions, 'bpmndi:BPMNDiagram', id=f'{process_id}_Diagram', name=title)
        self.plane = element(diagram, 'bpmndi:BPMNPlane', id=f'{process_id}_Plane', bpmnElement=plane_ref)
        self.nodes = {}
        self.boxes = {}
        self.lanes = {}
        self.counter = 0

    def shape(self, ident, box, **attrs):
        self.boxes[ident] = box
        shape = element(self.plane, 'bpmndi:BPMNShape', id=f'{ident}_di', bpmnElement=ident, **attrs)
        x, y, width, height = box
        element(shape, 'dc:Bounds', x=x, y=y, width=width, height=height)
        return shape

    def participant(self, ident, name, box, process=False):
        attrs = dict(id=ident, name=name)
        if process:
            attrs['processRef'] = self.process.attrib['id']
        element(self.collaboration, 'bpmn:participant', **attrs)
        self.shape(ident, box, isHorizontal='true')

    def lane(self, ident, name, box):
        lane_set = self.process.find(f'{{{NS["bpmn"]}}}laneSet')
        if lane_set is None:
            lane_set = element(self.process, 'bpmn:laneSet', id=f'{self.process.attrib["id"]}_Lanes')
        self.lanes[ident] = element(lane_set, 'bpmn:lane', id=ident, name=name)
        self.shape(ident, box, isHorizontal='true')

    def node(self, ident, kind, label, x, y, width=170, height=80, lane=None, **attrs):
        node = element(self.process, f'bpmn:{kind}', id=ident, name=label, **attrs)
        self.nodes[ident] = node
        if lane:
            element(self.lanes[lane], 'bpmn:flowNodeRef').text = ident
        shape_attrs = {}
        if kind in ('exclusiveGateway', 'parallelGateway'):
            shape_attrs['isMarkerVisible'] = 'true'
        shape = self.shape(ident, (x, y, width, height), **shape_attrs)
        if kind.endswith('Event') or kind.endswith('Gateway'):
            bpmn_label = element(shape, 'bpmndi:BPMNLabel')
            label_x = x - 15 if kind == 'startEvent' else x - 42
            element(bpmn_label, 'dc:Bounds', x=label_x, y=y+height+8, width=width+84, height=42)
        return node

    def event(self, ident, kind, label, x, y, definition=None, lane=None, **attrs):
        node = self.node(ident, kind, label, x, y, 36, 36, lane=lane, **attrs)
        if definition:
            event_kind, value = definition
            event = element(node, f'bpmn:{event_kind}EventDefinition', id=f'{ident}_definition')
            if event_kind == 'timer':
                duration = element(event, 'bpmn:timeDuration')
                duration.set(f'{{{NS["xsi"]}}}type', 'bpmn:tFormalExpression')
                duration.text = value
            elif event_kind == 'error':
                event.set('errorRef', value)
            elif event_kind == 'message':
                event.set('messageRef', value)
        return node

    def gateway(self, ident, label, x, y, parallel=False, lane=None):
        return self.node(ident, 'parallelGateway' if parallel else 'exclusiveGateway', label, x, y, 50, 50, lane=lane)

    def point(self, ident, side):
        x, y, w, h = self.boxes[ident]
        return {'left': (x, y+h/2), 'right': (x+w, y+h/2),
                'top': (x+w/2, y), 'bottom': (x+w/2, y+h)}[side]

    def edge(self, source, target, label='', source_side='right', target_side='left', via=(),
             condition=None, default=False, message=False, label_at=None, start_at=None, end_at=None):
        self.counter += 1
        ident = f'{self.process.attrib["id"]}_Flow_{self.counter}'
        parent = self.collaboration if message else self.process
        flow = element(parent, 'bpmn:messageFlow' if message else 'bpmn:sequenceFlow',
                       id=ident, sourceRef=source, targetRef=target, name=label)
        if not message:
            element(self.nodes[source], 'bpmn:outgoing').text = ident
            element(self.nodes[target], 'bpmn:incoming').text = ident
            if condition:
                expression = element(flow, 'bpmn:conditionExpression')
                expression.set(f'{{{NS["xsi"]}}}type', 'bpmn:tFormalExpression')
                expression.text = condition
            if default:
                self.nodes[source].set('default', ident)
        points = [start_at or self.point(source, source_side), *via, end_at or self.point(target, target_side)]
        edge = element(self.plane, 'bpmndi:BPMNEdge', id=f'{ident}_di', bpmnElement=ident)
        for x, y in points:
            element(edge, 'di:waypoint', x=x, y=y)
        if label:
            if label_at is None:
                label_at = ((points[0][0]+points[1][0])/2-60, (points[0][1]+points[1][1])/2-28)
            lab = element(edge, 'bpmndi:BPMNLabel')
            element(lab, 'dc:Bounds', x=label_at[0], y=label_at[1], width=120, height=38)
        return ident

    def note(self, ident, text, x, y, width, height):
        note = element(self.process, 'bpmn:textAnnotation', id=ident)
        element(note, 'bpmn:text').text = text
        self.shape(ident, (x, y, width, height))


main = Diagram('Process_Search', 'BPMN — показ блока', collaboration=True)
main.participant('User', 'Пользователь', (70, 70, 2700, 75))
main.participant('Search', 'Система формирования поисковой страницы', (70, 220, 2700, 1030), process=True)
main.lane('Sources', 'Оркестратор · получение ответов', (100, 220, 2670, 620))
main.lane('Blender', 'Верхний блендер и сборщик страницы', (100, 840, 2670, 410))
main.participant('Quality', 'Платформа обучения и оценки · разметка выборки, оценка по срезам, обучение и публикация моделей', (70, 1390, 2700, 85))
main.event('Request', 'startEvent', 'Получен запрос', 150, 485, ('message', 'Message_Request'), lane='Sources')
main.node('Context', 'serviceTask', 'Подготовить текст, язык и регион', 235, 465, lane='Sources')
main.gateway('Fork', '', 450, 480, parallel=True, lane='Sources')

# Each branch produces one status before joining, including timeout and error paths.
main.node('Web', 'serviceTask', 'Получить веб-ответ через кэш или поиск по индексу', 610, 280, 220, 85, lane='Sources')
main.event('WebTimer', 'boundaryEvent', '400 мс', 635, 347, ('timer', 'PT0.4S'), lane='Sources', attachedToRef='Web', cancelActivity='true')
main.event('WebError', 'boundaryEvent', 'Ошибка', 790, 347, ('error', 'Error_Service'), lane='Sources', attachedToRef='Web', cancelActivity='true')
main.node('WebFallback', 'serviceTask', 'Задать пустой веб-ответ и статус сбоя', 925, 365, 200, 80, lane='Sources')
main.gateway('WebMerge', '', 1210, 297, lane='Sources')

main.node('Geo', 'callActivity', 'Подготовить геопредложение\nСм. Process_Geo', 610, 490, 220, 85, lane='Sources', calledElement='Process_Geo')
main.event('GeoTimer', 'boundaryEvent', '300 мс', 635, 557, ('timer', 'PT0.3S'), lane='Sources', attachedToRef='Geo', cancelActivity='true')
main.event('GeoError', 'boundaryEvent', 'Ошибка', 790, 557, ('error', 'Error_Service'), lane='Sources', attachedToRef='Geo', cancelActivity='true')
main.node('GeoFallback', 'serviceTask', 'Задать пустое геопредложение и статус сбоя', 925, 575, 200, 80, lane='Sources')
main.gateway('GeoMerge', '', 1210, 507, lane='Sources')

other = main.node('Others', 'serviceTask', 'Получить предложение вертикали\nКино, энциклопедия, …', 610, 700, 220, 85, lane='Sources')
element(other, 'bpmn:multiInstanceLoopCharacteristics', isSequential='false')
main.event('OtherTimer', 'boundaryEvent', '300 мс', 635, 767, ('timer', 'PT0.3S'), lane='Sources', attachedToRef='Others', cancelActivity='true')
main.event('OtherError', 'boundaryEvent', 'Ошибка', 790, 767, ('error', 'Error_Service'), lane='Sources', attachedToRef='Others', cancelActivity='true')
main.node('OtherFallback', 'serviceTask', 'Взять накопленные ответы; недоступные пометить пустыми', 1440, 710, 220, 85, lane='Sources')
main.gateway('OtherMerge', '', 1740, 717, lane='Sources')
main.gateway('Join', '', 1870, 507, parallel=True, lane='Sources')
main.note('MainNote', 'Вертикали вызываются параллельно.\nОтветы сохраняются по request_id.\nПри сборке у каждой ветки есть ответ или статус сбоя.', 2250, 310, 450, 180)

main.edge('Request', 'Context')
main.edge('Context', 'Fork')
main.edge('Fork', 'Web', source_side='top', via=((475, 322.5),))
main.edge('Fork', 'Geo', via=((550, 505), (550, 532.5)))
main.edge('Fork', 'Others', source_side='bottom', via=((475, 742.5),))
for task, timer, error, fallback, merge in [
    ('Web', 'WebTimer', 'WebError', 'WebFallback', 'WebMerge'),
    ('Geo', 'GeoTimer', 'GeoError', 'GeoFallback', 'GeoMerge'),
]:
    main.edge(task, merge)
    main.edge(timer, fallback, source_side='bottom', via=((653, main.point(fallback, 'left')[1]),))
    main.edge(error, fallback, source_side='right', via=((870, main.point(error, 'right')[1]), (870, main.point(fallback, 'left')[1])))
    main.edge(fallback, merge, target_side='bottom', via=((1235, main.point(fallback, 'right')[1]),))
main.edge('Others', 'OtherMerge', target_side='top', via=((870, 742.5), (870, 680), (1765, 680)))
main.edge('OtherTimer', 'OtherFallback', source_side='bottom', target_side='bottom', via=((653, 825), (1550, 825)))
main.edge('OtherError', 'OtherFallback', source_side='right', target_side='left', via=((1380, 785), (1380, 750)))
main.edge('OtherFallback', 'OtherMerge', target_side='bottom', via=((1765, 750),))
main.edge('WebMerge', 'Join', target_side='top', via=((1895, 322),))
main.edge('GeoMerge', 'Join', via=((1550, 532),))
main.edge('OtherMerge', 'Join', target_side='bottom', via=((1895, 742),))

main.gateway('HasBlocks', 'Есть валидные блоки?', 350, 935, lane='Blender')
main.node('Score', 'serviceTask', 'Посчитать общий скор полезности каждого блока', 520, 920, 220, 85, lane='Blender')
main.event('ScoreTimer', 'boundaryEvent', '50 мс', 545, 987, ('timer', 'PT0.05S'), lane='Blender', attachedToRef='Score', cancelActivity='true')
main.event('ScoreError', 'boundaryEvent', 'Ошибка', 700, 987, ('error', 'Error_Service'), lane='Blender', attachedToRef='Score', cancelActivity='true')
main.gateway('Threshold', 'Есть скор ≥ 0,70?', 870, 937, lane='Blender')
main.node('Choose', 'serviceTask', 'Выбрать блок с максимальным скором', 1060, 920, 205, 85, lane='Blender')
main.node('None', 'serviceTask', 'Зафиксировать отсутствие специального блока', 1050, 1125, 220, 80, lane='Blender')
main.gateway('DecisionMerge', '', 1400, 937, lane='Blender')
main.event('Log', 'intermediateThrowEvent', 'Опубликовать событие решения', 1550, 945, ('message', 'Message_Decision'), lane='Blender')
main.node('Render', 'serviceTask', 'Собрать страницу из доступных веб-результатов и блока', 1690, 920, 240, 85, lane='Blender')
main.node('Send', 'sendTask', 'Отправить страницу пользователю', 2040, 920, 200, 85, lane='Blender')
main.event('Done', 'endEvent', 'Страница отправлена', 2360, 945, lane='Blender')
main.edge('Join', 'HasBlocks', source_side='bottom', target_side='left', via=((1970, 600), (1970, 860), (285, 860), (285, 960)))
main.edge('HasBlocks', 'Score', 'Да', condition='valid_blocks_count > 0')
main.edge('HasBlocks', 'None', 'Нет', source_side='bottom', via=((375, 1165),), default=True, label_at=(385, 1050))
main.edge('Score', 'Threshold')
main.edge('ScoreTimer', 'None', source_side='bottom', via=((563, 1090), (1010, 1090), (1010, 1165)))
main.edge('ScoreError', 'None', source_side='bottom', via=((718, 1060), (990, 1060), (990, 1165)))
main.edge('Threshold', 'Choose', 'Да', condition='max_utility_score >= 0.70')
main.edge('Threshold', 'None', 'Нет', source_side='bottom', target_side='top', via=((895, 1080), (1160, 1080)), default=True, label_at=(915, 1030))
main.edge('Choose', 'DecisionMerge')
main.edge('None', 'DecisionMerge', target_side='bottom', via=((1425, 1165),))
main.edge('DecisionMerge', 'Log')
main.edge('Log', 'Render')
main.edge('Render', 'Send')
main.edge('Send', 'Done')
main.edge('User', 'Request', 'Запрос и контекст', source_side='bottom', target_side='top', start_at=(168, 145), message=True, label_at=(200, 170))
main.edge('Send', 'User', 'Готовая страница', source_side='top', target_side='bottom', end_at=(2140, 145), message=True, label_at=(2160, 700))
main.edge('Log', 'Quality', 'Все предложения, решение, версии', source_side='bottom', target_side='top', via=((1568, 1320), (1420, 1320)), message=True, label_at=(1575, 1290))
main.edge('User', 'Quality', 'Показы и действия после ответа', source_side='right', target_side='right', via=((2870, 107.5), (2870, 1432.5)), message=True, label_at=(2890, 880))

geo = Diagram('Process_Geo', 'BPMN — геопоиск', collaboration=True)
geo.participant('GeoPool', 'Геопоиск · вызывается задачей Process_Search.Geo', (70, 110, 3210, 1020), process=True)
geo.lane('Retrieval', 'Геопоиск · кандидаты и признаки', (100, 110, 3180, 500))
geo.lane('Models', 'Геопоиск и ML-сервис · оценка и подготовка ответа', (100, 610, 3180, 520))
geo.event('GeoStart', 'startEvent', 'Получен контекст вызова', 145, 320, lane='Retrieval')
geo.gateway('GeoFork', '', 270, 313, parallel=True, lane='Retrieval')
geo.node('Key', 'serviceTask', 'Построить ключ кэша\nЗапрос, язык, область, версия индекса', 385, 170, 210, 85, lane='Retrieval')
geo.node('ReadCache', 'serviceTask', 'Проверить геокэш кандидатов', 680, 170, 190, 85, lane='Retrieval')
geo.gateway('CacheHit', 'Есть свежая запись?', 950, 187, lane='Retrieval')
geo.node('Cached', 'serviceTask', 'Получить ID из кэша', 1100, 170, 180, 85, lane='Retrieval')
geo.node('Retrieve', 'serviceTask', 'Найти кандидатов по инвертированному индексу', 1080, 350, 220, 85, lane='Retrieval')
geo.node('WriteCache', 'serviceTask', 'Записать ID кандидатов в кэш', 1370, 350, 190, 85, lane='Retrieval')
geo.gateway('CacheMerge', '', 1660, 187, lane='Retrieval')
geo.node('Features', 'serviceTask', 'Получить актуальные признаки организаций и топонимов', 1790, 170, 230, 85, lane='Retrieval')
geo.node('Classify', 'serviceTask', 'ML · оценить геоинтент, org1/manyorg и признаки запроса', 1080, 500, 220, 85, lane='Retrieval')
geo.gateway('FeaturesJoin', '', 2140, 313, parallel=True, lane='Retrieval')
geo.node('Filter', 'serviceTask', 'ML · применить ранние фильтры по запросу, контексту и объекту', 2310, 298, 230, 85, lane='Retrieval')
geo.gateway('Candidates', 'Есть кандидаты?', 2660, 315, lane='Retrieval')
geo.note('GeoNote', 'org1/manyorg определяется для каждого запроса.\nЧисло найденных объектов влияет на формат блока.\nВеб-признаки поступают только в верхний блендер.\nМодели используют совместимые схемы признаков.', 2800, 180, 390, 180)

geo.node('Rank', 'serviceTask', 'ML · ранжировать сокращённый набор и применить фильтр v2', 385, 725, 240, 90, lane='Models')
geo.gateway('Survived', 'Есть прошедшие фильтр?', 750, 745, lane='Models')
geo.node('Empty', 'serviceTask', 'Вернуть статус empty и признаки запроса', 1060, 980, 220, 80, lane='Models')
geo.gateway('Format', 'Какой формат?', 1060, 745, lane='Models')
geo.node('Single', 'serviceTask', 'Собрать карточку топ-1', 1250, 655, 220, 85, lane='Models')
geo.node('Many', 'serviceTask', 'Собрать список лучших организаций', 1250, 805, 220, 85, lane='Models')
geo.gateway('FormatMerge', '', 1590, 745, lane='Models')
geo.node('Offer', 'serviceTask', 'Вернуть геопредложение\nСодержимое, признаки, версии и статус ok', 1770, 725, 250, 90, lane='Models')
geo.gateway('ReturnMerge', '', 2230, 745, lane='Models')
geo.event('GeoEnd', 'endEvent', 'Геоветвь завершена', 2460, 752, lane='Models')
geo.note('FailureNote', 'Ошибки ML или индекса передаются вызывающему процессу.\nГраничные Error и Timer у задачи Geo в основной схеме\nпревращают сбой в пустое предложение со статусом.\nНедоступность кэша считается cache miss. Запись best effort.', 2730, 770, 430, 180)

geo.edge('GeoStart', 'GeoFork')
geo.edge('GeoFork', 'Key', source_side='top', via=((295, 212),))
geo.edge('GeoFork', 'Classify', source_side='bottom', via=((295, 542.5),))
geo.edge('Key', 'ReadCache')
geo.edge('ReadCache', 'CacheHit')
geo.edge('CacheHit', 'Cached', 'Да', condition='cache_hit and cache_version_matches')
geo.edge('CacheHit', 'Retrieve', 'Нет / сбой', source_side='bottom', via=((975, 392.5),), default=True, label_at=(965, 290))
geo.edge('Retrieve', 'WriteCache')
geo.edge('Cached', 'CacheMerge')
geo.edge('WriteCache', 'CacheMerge', target_side='bottom', via=((1685, 392.5),))
geo.edge('CacheMerge', 'Features')
geo.edge('Features', 'FeaturesJoin', target_side='top', via=((2165, 212.5),))
geo.edge('Classify', 'FeaturesJoin', target_side='bottom', via=((2165, 542.5),))
geo.edge('FeaturesJoin', 'Filter')
geo.edge('Filter', 'Candidates')
geo.edge('Candidates', 'Rank', 'Да', source_side='bottom', via=((2685, 640), (330, 640), (330, 770)), condition='candidate_count > 0', label_at=(2550, 545))
geo.edge('Candidates', 'Empty', 'Нет', source_side='right', target_side='bottom', via=((2720, 340), (2720, 1090), (1170, 1090)), default=True, label_at=(2700, 1030))
geo.edge('Rank', 'Survived')
geo.edge('Survived', 'Format', 'Да', condition='survivor_count > 0')
geo.edge('Survived', 'Empty', 'Нет', source_side='bottom', via=((775, 1020),), default=True, label_at=(790, 920))
geo.edge('Format', 'Single', 'org1 или 1 объект', source_side='top', via=((1085, 697.5),), condition='request_kind == "org1" or survivor_count == 1', label_at=(1090, 660))
geo.edge('Format', 'Many', 'manyorg и ≥ 2', source_side='bottom', via=((1085, 847.5),), default=True, label_at=(1090, 850))
geo.edge('Single', 'FormatMerge', target_side='top', via=((1615, 697.5),))
geo.edge('Many', 'FormatMerge', target_side='bottom', via=((1615, 847.5),))
geo.edge('FormatMerge', 'Offer')
geo.edge('Offer', 'ReturnMerge')
geo.edge('Empty', 'ReturnMerge', target_side='bottom', via=((2255, 1020),))
geo.edge('ReturnMerge', 'GeoEnd')

# Keep external labels clear of the outgoing branches and boundary-event flows.
def position_label(diagram, ident, x, y, width, height):
    shape = diagram.plane.find(f"bpmndi:BPMNShape[@bpmnElement='{ident}']", NS)
    bounds = shape.find('bpmndi:BPMNLabel/dc:Bounds', NS)
    bounds.attrib.update(x=str(x), y=str(y), width=str(width), height=str(height))


for ident, bounds in {
    'Request': (139, 535, 90, 45),
    'WebTimer': (565, 385, 65, 24),
    'WebError': (785, 415, 70, 24),
    'GeoTimer': (565, 595, 65, 24),
    'GeoError': (785, 625, 70, 24),
    'OtherTimer': (565, 805, 65, 24),
    'OtherError': (790, 805, 75, 24),
    'HasBlocks': (235, 1000, 115, 45),
    'ScoreTimer': (480, 1030, 65, 24),
    'ScoreError': (735, 1030, 75, 24),
    'Threshold': (910, 1000, 125, 45),
    'Log': (1430, 1005, 120, 50),
}.items():
    position_label(main, ident, *bounds)

for ident, bounds in {
    'GeoStart': (135, 372, 120, 45),
    'CacheHit': (820, 247, 130, 45),
    'Candidates': (2490, 385, 155, 40),
    'Survived': (610, 810, 140, 45),
    'Format': (940, 710, 120, 32),
}.items():
    position_label(geo, ident, *bounds)


ET.indent(definitions, space='  ')
ET.ElementTree(definitions).write(ROOT/'search-process.bpmn', encoding='utf-8', xml_declaration=True)
for process_id, filename in [('Process_Search', 'bpmn-search.bpmn'), ('Process_Geo', 'bpmn-geosearch.bpmn')]:
    clone = ET.fromstring(ET.tostring(definitions))
    diagrams = clone.findall(f'{{{NS["bpmndi"]}}}BPMNDiagram')
    for diagram in diagrams:
        clone.remove(diagram)
    diagrams.sort(key=lambda diagram: diagram.attrib['id'] != f'{process_id}_Diagram')
    clone.extend(diagrams)
    ET.ElementTree(clone).write(ROOT/filename, encoding='utf-8', xml_declaration=True)
print('Wrote search-process.bpmn with two linked BPMN diagrams')
