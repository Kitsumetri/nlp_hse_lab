import gradio as gr

theme = gr.themes.Soft(
    primary_hue="teal",
    secondary_hue="gray",
    font=[gr.themes.GoogleFont("Libre Franklin"), "Arial", "sans-serif"]
)

def lanch_language_model_server(callback_generate_response, share: bool = False):
    with gr.Blocks(theme=theme, title="LLM Playground") as demo:
        gr.Markdown("# 🧠 Language Model Playground")
        gr.Markdown("Configure generation parameters and experiment with the model!")
        
        with gr.Row():
            # Левая колонка с вводом и выводом
            with gr.Column(scale=3):
                input_prompt = gr.Textbox(
                    label="Input Prompt",
                    placeholder="Enter your text here...",
                    lines=8,
                    elem_id="prompt-box"
                )
                prefix = gr.Textbox(
                    label="Instruction Prefix",
                    value="Резюмируй текст, сохрани важную информацию о нём в кратком объёме.",
                    placeholder="Add custom instruction prefix..."
                )
                examples = gr.Examples(
                    examples=[
                        ['В Екатеринбурге на 82-м году жизни скончался советский и российский детский писатель Владислав Крапивин. Об этом сообщили представители Минздрава региона. «Да. Сегодня», — цитирует пресс-службу Минздрава РИА «Новости». Согласно невестке Крапивина Ларисе, писатель умер утром 1 сентября по местному времени. «Умер в 06:40. Ему стало хуже, ночью отвезли в реанимацию — и все», — передает ее слова ТАСС. Ранее Лариса в своем Facebook рассказывала, что Крапивин был госпитализирован 15 июля. Как тогда подчеркивала родственница, решение об отправке писателя в госпиталь было принято на фоне того, что он «внезапно упал утром, встав с кровати». «Он был в сознании, хотя и плохо разговаривал. И было ощущение, что это инсульт. Была срочно вызвана скорая помощь. [В больнице ему] сделали КТ головного мозга и легких, кардиограмму и анализ крови, семье было сказано, что инсульта и инфаркта нет, но есть подозрение на COVID, потому что КТ легких показала наличие пневмонии с 25% поражения», — подчеркнула невестка автора. Она также отметила, что Крапивин, тесты которого на коронавирус изначально были отрицательными, некоторое время провел в COVID-отделении. Кроме того, женщина выразила мнение, что врачи «просмотрели [у писателя] инсульт, который, видимо, все-таки случился». Согласно родственнице, знаменитый автор был выписан из больницы 7 августа, при этом его состояние было «намного хуже, чем три недели назад». «Он практически вообще не может двигаться. У него на ноге огромная гематома — и вся нога синяя. В районе паха большая шишка. На спине пролежни. Все это вызывает адскую боль и мучения. Он все время просит пить. Ест с большим трудом, потому что идет рвота. С нами общается, но больше спит», — делилась Лариса 9 августа. Уже на следующий день писатель был вновь госпитализирован. А 12 августа Крапивину была проведена операция: «Говорят, что из ноги хирурги откачали 1,5 литра нехорошей скопившейся жидкости». Впоследствии Лариса писала, что после краткосрочного ухудшения состояния Крапивин почувствовал себя лучше. Вместе с тем 31 августа женщина, ссылаясь на врачей, заявила Ura.Ru , что писатель все же переболел коронавирусом, который и «спровоцировал осложнение всех старых болячек, включая диабет, сердце и инсульты». По информации источника, причиной смерти автора стало обострение хронических болезней на фоне COVID-19. Соболезнования в связи с кончиной писателя выразил губернатор Свердловской области Евгений Куйвашев. «Это тяжелая, скорбная, невосполнимая утрата для всех нас. Владислав Крапивин широко известен как автор замечательных произведений для детей и юношества. Почти в каждой российской семье есть книги Владислава Петровича, утверждающие идеалы добра и справедливости, проникнутые духом романтизма и приключений, поднимающие важнейшие жизненные и нравственные проблемы, — говорится в сообщении на портале губернатора. — Владислава Петровича Крапивина всегда будут помнить, как невероятно талантливого, честного, неравнодушного ко всякой несправедливости человека. Им восхищались, ему верили, его уважали и любили». Владислав Крапивин родился 14 октября 1938 года в Тюмени. На его счету такие произведения, как «Мальчик со шпагой», «Журавленок и молнии», «Трое с площади Карронад», «Колыбельная для брата» и «Болтик», работы автора также неоднократно экранизировались. Помимо литературной деятельности, Крапивин известен как педагог: в начале 1960-х он организовал в Свердловске детский отряд «Каравелла», которым впоследствии руководил несколько десятилетий.', "Резюмируй текст, сохрани важную информацию о нём в кратком объёме"],
                    ],
                    inputs=[input_prompt, prefix],
                    label="Example Prompts"
                )
                output_text = gr.Textbox(
                    label="Generated Output",
                    elem_id="output-box",
                    interactive=False,
                    lines=12
                )
            
            # Правая колонка с параметрами и кнопкой
            with gr.Column(scale=1):
                with gr.Accordion("Generation Parameters", open=True):
                    max_new_tokens = gr.Slider(
                        label="Max New Tokens",
                        minimum=32,
                        maximum=256,
                        value=512,
                        step=16
                    )
                    temperature = gr.Slider(
                        label="Temperature",
                        minimum=0.1,
                        maximum=1.0,
                        value=0.7,
                        step=0.1
                    )
                    top_p = gr.Slider(
                        label="Top-p (Nucleus Sampling)",
                        minimum=0.1,
                        maximum=1.0,
                        value=0.95,
                        step=0.05
                    )
                    top_k = gr.Slider(
                        label="Top-k",
                        minimum=1,
                        maximum=100,
                        value=50,
                        step=1
                    )
                    repetition_penalty = gr.Slider(
                        label="Repetition Penalty",
                        minimum=0.1,
                        maximum=1.9,
                        value=1.2,
                        step=0.1
                    )
                    num_beams = gr.Slider(
                        label="Beam Search Size",
                        minimum=1,
                        maximum=8,
                        value=4,
                        step=1,
                        visible=False
                    )
                    do_sample = gr.Checkbox(
                        label="Enable Sampling",
                        value=True
                    )
                    skip_special = gr.Checkbox(
                        label="Skip Special Tokens",
                        value=True
                    )
                
                submit_btn = gr.Button("Generate", variant="primary")
        
        # Обработчики событий
        do_sample.change(
            lambda x: gr.update(visible=not x),
            inputs=do_sample,
            outputs=num_beams
        )
        
        submit_btn.click(
            callback_generate_response,
            inputs=[
                input_prompt,
                prefix,
                max_new_tokens,
                temperature,
                top_p,
                top_k,
                repetition_penalty,
                skip_special,
                num_beams,
                do_sample
            ],
            outputs=output_text
        )
        
        input_prompt.submit(
            callback_generate_response,
            inputs=[
                input_prompt,
                prefix,
                max_new_tokens,
                temperature,
                top_p,
                top_k,
                repetition_penalty,
                skip_special,
                num_beams,
                do_sample
            ],
            outputs=output_text
        )
    
    demo.launch(share=share)